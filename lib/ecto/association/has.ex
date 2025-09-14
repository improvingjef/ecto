defmodule Ecto.Association.Has do
  import Ecto.Query, only: [from: 2]
  import Ecto.Association.Options
  use Ecto.Changeset.Relation

  # ## Relation callbacks
  # @behaviour Ecto.Changeset.Relation

  # @impl true
  # def build(%{related: related, queryable: queryable, defaults: defaults}, owner) do
  #   related
  #   |> Ecto.Association.apply_defaults(defaults, owner)
  #   |> Ecto.Association.merge_source(queryable)

  @moduledoc """
  The association struct for `has_one` and `has_many` associations.

  Its fields are:

    * `cardinality` - The association cardinality
    * `field` - The name of the association field on the schema
    * `owner` - The schema where the association was defined
    * `related` - The schema that is associated
    * `owner_key` - The key on the `owner` schema used for the association
    * `related_key` - The key on the `related` schema used for the association
    * `queryable` - The real query to use for querying association
    * `on_delete` - The action taken on associations when schema is deleted
    * `on_replace` - The action taken on associations when schema is replaced
    * `defaults` - Default fields used when building the association
    * `relationship` - The relationship to the specified schema, default is `:child`
    * `preload_order` - Default `order_by` of the association, used only by preload
  """
  @doc false
  def __define__(mod, cardinality, name, queryable, opts, fun_arity)
      when cardinality in [:one, :many] do
    if is_list(queryable) and Keyword.has_key?(queryable, :through) do
      check!(:has, queryable, fun_arity)
      association(mod, cardinality, name, Ecto.Association.HasThrough, queryable)
    else
      check!(:has, opts, fun_arity)

      struct =
        association(mod, cardinality, name, Ecto.Association.Has, [queryable: queryable] ++ opts)

      Module.put_attribute(mod, :ecto_changeset_fields, {name, {:assoc, struct}})
    end
  end

  @behaviour Ecto.Association
  @on_delete_opts [:nothing, :nilify_all, :delete_all]
  @on_replace_opts [:raise, :mark_as_invalid, :delete, :delete_if_exists, :nilify]
  @has_one_on_replace_opts @on_replace_opts ++ [:update]
  defstruct [
    :cardinality,
    :field,
    :owner,
    :related,
    :owner_key,
    :related_key,
    :on_cast,
    :queryable,
    :on_delete,
    :on_replace,
    where: [],
    unique: true,
    defaults: [],
    relationship: :child,
    ordered: false,
    preload_order: []
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
    dbg({"default", opts})
    opts = opts
    |> Keyword.put(:owner, module)
    |> Keyword.put(:field, name)
    |> Keyword.put_new(:where, [])
    |> Keyword.put_new(:defaults, [])
    |> Keyword.put_new(:preload_order, [])
    |> Keyword.put_new(:on_delete, :nothing)
    |> Keyword.put_new(:on_replace, :raise)

    queryable = Keyword.fetch!(opts, :queryable)
    cardinality = Keyword.fetch!(opts, :cardinality)
    related = Ecto.Association.related_from_query(queryable, name)

    ref =
      module
      |> Module.get_attribute(:primary_key)
      |> get_ref(opts[:references], name)

    unless Module.get_attribute(module, :ecto_fields)[ref] do
      raise ArgumentError,
            "schema does not have the field #{inspect(ref)} used by " <>
              "association #{inspect(name)}, please set the :references option accordingly"
    end

    if opts[:through] do
      raise ArgumentError,
            "invalid association #{inspect(name)}. When using the :through " <>
              "option, the schema should not be passed as second argument"
    end

    on_delete = Keyword.get(opts, :on_delete, :nothing)
    dbg({"on_delete", on_delete, @on_delete_opts})
    unless on_delete in @on_delete_opts do
      raise ArgumentError,
            "invalid :on_delete option for #{inspect(name)}. " <>
              "The only valid options are: " <>
              Enum.map_join(@on_delete_opts, ", ", &"`#{inspect(&1)}`")
    end

    on_replace = Keyword.get(opts, :on_replace, :raise)
    on_replace_opts = if cardinality == :one, do: @has_one_on_replace_opts, else: @on_replace_opts

    unless on_replace in on_replace_opts do
      raise ArgumentError,
            "invalid `:on_replace` option for #{inspect(name)}. " <>
              "The only valid options are: " <>
              Enum.map_join(@on_replace_opts, ", ", &"`#{inspect(&1)}`")
    end

    defaults = Ecto.Association.validate_defaults!(module, name, opts[:defaults] || [])
    preload_order = Ecto.Association.validate_preload_order!(name, opts[:preload_order] || [])
    where = opts[:where] || []

    unless is_list(where) do
      raise ArgumentError,
            "expected `:where` for #{inspect(name)} to be a keyword list, got: `#{inspect(where)}`"
    end

    opts = Enum.reduce(opts, [], fn {option, value}, options ->
      opt_in(option, Keyword.merge(opts, options), module, name) ++ options
    end)

    dbg(opts)

    %__MODULE__{
      field: opts[:field],
      cardinality: opts[:cardinality],
      owner: opts[:owner],
      related: related,
      owner_key: ref,
      queryable: opts[:queryable],
      on_delete: on_delete,
      on_replace: on_replace,
      defaults: defaults,
      where: where,
      preload_order: preload_order,
      related_key: opts[:foreign_key] || Ecto.Association.association_key(module, ref)
    }
  end

  # @impl true
  def struct2(module, name, opts) do
    opts = opts
    |> Keyword.put(:owner, module)
    |> Keyword.put(:field, name)
    |> Keyword.put(:on_delete, opts[:on_delete] || :nothing)
    |> Keyword.put(:on_replace, opts[:on_replace] ||:raise)
    |> Keyword.put_new(:where, [])
    |> Keyword.put_new(:defaults, [])
    |> Keyword.put_new(:preload_order, [])

    opts = Enum.reduce(opts, [], fn {option, value}, options ->
      dbg(opt_in(option, Keyword.merge(opts, options), module, name) ++ options)

    end)


    struct(__MODULE__, opts) # ++ [related_key: opts[:foreign_key] || Ecto.Association.association_key(module, ref), owner_key: ref])

  end

  def opt_in(:references, options, module, name) do
    ref =
      module
      |> Module.get_attribute(:primary_key)
      |> get_ref(options[:references], name)

    unless Module.get_attribute(module, :ecto_fields)[ref] do
      raise ArgumentError,
            "schema does not have the field #{inspect(ref)} used by " <>
              "association #{inspect(name)}, please set the :references option accordingly"
    end

    [owner_key: ref, related_key: options[:foreign_key] || Ecto.Association.association_key(module, ref)]
  end

  def opt_in(:where, options, _module, name) do
    unless is_list(options[:where]) do
      raise ArgumentError,
            "expected `:where` for #{inspect(name)} to be a keyword list, got: `#{inspect(options[:where])}`"
    end
    [where: options[:where]]
  end
  # if :through exists, raise an error
  def opt_in(:through, _options, _module, name) do
    raise ArgumentError,
          "invalid association #{inspect(name)}. When using the :through " <>
            "option, the schema should not be passed as second argument"
  end

  def opt_in(:preload_order, options, _module, name) do
    preload_order = Ecto.Association.validate_preload_order!(name, options[:preload_order])
    [preload_order: preload_order]
  end

  def opt_in(:defaults, options, module, name) do
    defaults = Ecto.Association.validate_defaults!(module, name, options[:defaults])
    [defaults: defaults]
  end

  def opt_in(:queryable, options, _module, name) do
    queryable = Keyword.fetch!(options, :queryable)
    related = Ecto.Association.related_from_query(queryable, name)
    [queryable: queryable, related: related]
  end

  def opt_in(:on_delete, options, _module, name) do
    on_delete = options[:on_delete]

    unless on_delete in @on_delete_opts do
      raise ArgumentError,
            "invalid :on_delete option for #{inspect(name)}. " <>
              "The only valid options are: " <>
              Enum.map_join(@on_delete_opts, ", ", &"`#{inspect(&1)}`")
    end
    [on_delete: on_delete]
  end

  def opt_in(:on_replace, options, _module, name) do
    cardinality = Keyword.fetch!(options, :cardinality)
    on_replace = options[:on_replace]
    on_replace_opts = if cardinality == :one, do: @has_one_on_replace_opts, else: @on_replace_opts

    unless on_replace in on_replace_opts do
      raise ArgumentError,
            "invalid `:on_replace` option for #{inspect(name)}. " <>
              "The only valid options are: " <>
              Enum.map_join(@on_replace_opts, ", ", &"`#{inspect(&1)}`")
    end
    [on_replace: on_replace]
  end

  def opt_in(option, options, _module, _name) do
    Keyword.put([], option, options[option])
  end

  defp get_ref(primary_key, nil, name) when primary_key in [nil, false] do
    raise ArgumentError,
          "need to set :references option for " <>
            "association #{inspect(name)} when schema has no primary key"
  end

  defp get_ref(primary_key, nil, _name), do: elem(primary_key, 0)
  defp get_ref(_primary_key, references, _name), do: references

  @impl true
  def build(%{owner_key: owner_key, related_key: related_key} = refl, owner, attributes) do
    data = refl |> build(owner) |> struct(attributes)
    %{data | related_key => Map.get(owner, owner_key)}
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
        %{data: parent} = parent_changeset,
        %{action: :replace} = changeset,
        adapter,
        opts
      ) do
    changeset =
      case on_replace do
        :nilify -> %{changeset | action: :update}
        :update -> %{changeset | action: :update}
        :delete -> %{changeset | action: :delete}
      end

    changeset = Ecto.Association.update_parent_prefix(changeset, parent)

    case on_repo_change(refl, %{parent_changeset | data: nil}, changeset, adapter, opts) do
      {:ok, _} -> {:ok, nil}
      {:error, changeset} -> {:error, changeset}
    end
  end

  def on_repo_change(assoc, parent_changeset, changeset, _adapter, opts) do
    %{data: parent, repo: repo} = parent_changeset
    %{action: action, changes: changes} = changeset

    {key, value} = parent_key(assoc, parent)
    changeset = update_parent_key(changeset, action, key, value)
    changeset = Ecto.Association.update_parent_prefix(changeset, parent)

    case apply(repo, action, [changeset, opts]) do
      {:ok, _} = ok ->
        if action == :delete, do: {:ok, nil}, else: ok

      {:error, changeset} ->
        original = Map.get(changes, key)
        {:error, put_in(changeset.changes[key], original)}
    end
  end

  defp update_parent_key(changeset, :delete, _key, _value),
    do: changeset

  defp update_parent_key(changeset, _action, key, value),
    do: Ecto.Changeset.put_change(changeset, key, value)

  defp parent_key(%{related_key: related_key}, nil) do
    {related_key, nil}
  end

  defp parent_key(%{owner_key: owner_key, related_key: related_key}, owner) do
    {related_key, Map.get(owner, owner_key)}
  end

  # end

  ## On delete callbacks

  @doc false
  def delete_all(refl, parent, repo_name, opts) do
    if query = on_delete_query(refl, parent) do
      Ecto.Repo.Queryable.delete_all(repo_name, query, opts)
    end
  end

  @doc false
  def nilify_all(%{related_key: related_key} = refl, parent, repo_name, opts) do
    if query = on_delete_query(refl, parent) do
      Ecto.Repo.Queryable.update_all(repo_name, query, [set: [{related_key, nil}]], opts)
    end
  end

  defp on_delete_query(
         %{owner_key: owner_key, related_key: related_key, queryable: queryable},
         parent
       ) do
    if value = Map.get(parent, owner_key) do
      query = from x in queryable, where: field(x, ^related_key) == ^value

      parent
      |> Ecto.get_meta(:prefix)
      |> case do
        nil -> query
        prefix -> Ecto.Query.put_query_prefix(query, prefix)
      end
    end
  end
end
