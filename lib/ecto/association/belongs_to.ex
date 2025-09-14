defmodule Ecto.Association.BelongsTo do
  import Ecto.Query, only: [from: 2]
  import Ecto.Association.Options, only: [check!: 4, association: 5]
  use Ecto.Changeset.Relation

  @moduledoc """
  The association struct for a `belongs_to` association.

  Its fields are:

    * `cardinality` - The association cardinality
    * `field` - The name of the association field on the schema
    * `owner` - The schema where the association was defined
    * `owner_key` - The key on the `owner` schema used for the association
    * `related` - The schema that is associated
    * `related_key` - The key on the `related` schema used for the association
    * `queryable` - The real query to use for querying association
    * `defaults` - Default fields used when building the association
    * `relationship` - The relationship to the specified schema, default `:parent`
    * `on_replace` - The action taken on associations when schema is replaced
  """

  # :primary_key is valid here to support associative entity
  # https://en.wikipedia.org/wiki/Associative_entity
  #
  @doc false
  def __define__(mod, name, queryable, opts) do
    opts = opts
    |> Keyword.put_new(:foreign_key, :"#{name}_id")
    |> Keyword.put_new(:type, Module.get_attribute(mod, :foreign_key_type, :id))
    |> Keyword.put_new(:define_field, true)

    foreign_key_type = Ecto.Schema.Field.check_field_type!(mod, name, opts[:type], opts)
    check!(:belongs_to, foreign_key_type, opts, "belongs_to/3")

    if opts[:foreign_key] == name do
      raise ArgumentError,
            "foreign_key #{inspect(name)} must be distinct from corresponding association name"
    end

    if opts[:define_field] do
      Module.put_attribute(mod, :ecto_changeset_fields, {opts[:foreign_key], foreign_key_type})
      Ecto.Schema.Field.define_field(mod, opts[:foreign_key], foreign_key_type, opts)
    end

    struct =
      association(mod, :one, name, Ecto.Association.BelongsTo, [queryable: queryable] ++ opts)

    Module.put_attribute(mod, :ecto_changeset_fields, {name, {:assoc, struct}})
  end

  @behaviour Ecto.Association
  @impl true
  def build(refl, owner, attributes) do
    refl
    |> build(owner)
    |> struct(attributes)
  end

  @on_replace_opts [:raise, :mark_as_invalid, :delete, :delete_if_exists, :nilify, :update]
  defstruct [
    :field,
    :owner,
    :related,
    :owner_key,
    :related_key,
    :queryable,
    :on_cast,
    :on_replace,
    where: [],
    defaults: [],
    cardinality: :one,
    relationship: :parent,
    unique: true,
    ordered: false
  ]


  @impl true
  def after_verify_validation(%{queryable: queryable, related_key: related_key}) do
    cond do
      not is_atom(queryable) ->
        :ok

      not Code.ensure_loaded?(queryable) ->
        {:error, "associated schema #{inspect(queryable)} does not exist"}

      not function_exported?(queryable, :__schema__, 2) ->
        {:error, "associated module #{inspect(queryable)} is not an Ecto schema"}

      is_nil(queryable.__schema__(:type, related_key)) ->
        {:error, "associated schema #{inspect(queryable)} does not have field `#{related_key}`"}

      true ->
        :ok
    end
  end

  @impl true
  def struct(module, name, opts) do
    opts = opts
    |> Keyword.put_new(:references, :id)
    |> Keyword.put_new(:on_replace, :raise)
    |> Keyword.put_new(:defaults, [])
    |> Keyword.put_new(:where, [])
    |> Keyword.put_new(:on_delete, :raise)
    |> Keyword.put_new(:field, name)
    |> Keyword.put_new(:owner, module)

    opts = Enum.reduce(opts, opts, fn {option, value}, options -> Keyword.merge(options, opt_in(option, options, module, name)) end)
  dbg(opts)
    struct(__MODULE__, opts)
  end

  # TODO: normalize :foreign_key and :owner_key
  def opt_in(:foreign_key, options, _, _) do
    [owner_key: options[:foreign_key]]
  end

  def opt_in(:queryable, options, _module, name) do
    [queryable: options[:queryable], related: Ecto.Association.related_from_query(options[:queryable], name)]
  end

  def opt_in(:defaults, options, module, name) do
    [defaults: Ecto.Association.validate_defaults!(module, name, options[:defaults])]
  end

  def opt_in(:on_replace, options, _module, name) do
    on_replace = options[:on_replace]
    unless on_replace in @on_replace_opts do
      raise ArgumentError,
            "invalid `:on_replace` option for #{inspect(name)}. " <>
              "Got: #{inspect(on_replace)}. The only valid options are: " <>
              Enum.map_join(@on_replace_opts, ", ", &"`#{inspect(&1)}`")
    end
    [on_replace: on_replace]
  end

  # TODO: normalize :references and :related_key
  def opt_in(:references, options, _, _) do
    [related_key: options[:references]]
  end

  def opt_in(option, options, _, _) do
    Keyword.put([], option, options[option])
  end

  @impl true
  def joins_query(
        %{related_key: related_key, owner: owner, owner_key: owner_key, queryable: queryable} =
          assoc
      ) do
    from(o in owner, join: q in ^queryable, on: field(q, ^related_key) == field(o, ^owner_key))
    |> Ecto.Association.combine_joins_query(assoc.where, 1)
  end

  @impl true
  def assoc_query(%{related_key: related_key, queryable: queryable} = assoc, query, [value]) do
    from(x in (query || queryable), where: field(x, ^related_key) == ^value)
    |> Ecto.Association.combine_assoc_query(assoc.where)
  end

  @impl true
  def assoc_query(%{related_key: related_key, queryable: queryable} = assoc, query, values) do
    from(x in (query || queryable), where: field(x, ^related_key) in ^values)
    |> Ecto.Association.combine_assoc_query(assoc.where)
  end

  @impl true
  def preload_info(%{related_key: related_key} = refl) do
    {:assoc, refl, {0, related_key}}
  end

  @impl true
  def on_repo_change(%{on_replace: :nilify}, _, %{action: :replace}, _adapter, _opts) do
    {:ok, nil}
  end

  def on_repo_change(
        %{on_replace: :delete_if_exists} = refl,
        parent_changeset,
        %{action: :replace} = changeset,
        adapter,
        opts
      ) do
    try do
      on_repo_change(%{refl | on_replace: :delete}, parent_changeset, changeset, adapter, opts)
    rescue
      Ecto.StaleEntryError -> {:ok, nil}
    end
  end

  def on_repo_change(
        %{on_replace: on_replace} = refl,
        parent_changeset,
        %{action: :replace} = changeset,
        adapter,
        opts
      ) do
    changeset =
      case on_replace do
        :delete -> %{changeset | action: :delete}
        :update -> %{changeset | action: :update}
      end

    on_repo_change(refl, parent_changeset, changeset, adapter, opts)
  end

  def on_repo_change(
        _refl,
        %{data: parent, repo: repo},
        %{action: action} = changeset,
        _adapter,
        opts
      ) do
    changeset = Ecto.Association.update_parent_prefix(changeset, parent)

    case apply(repo, action, [changeset, opts]) do
      {:ok, _} = ok ->
        if action == :delete, do: {:ok, nil}, else: ok

      {:error, changeset} ->
        {:error, changeset}
    end
  end
end
