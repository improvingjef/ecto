defmodule Ecto.Association.ManyToMany do
  import Ecto.Query, only: [from: 2, where: 3]
  import Ecto.Association.Options, only: [check!: 3, association: 5]
  use Ecto.Changeset.Relation

  @moduledoc """
  The association struct for `many_to_many` associations.

  Its fields are:

    * `cardinality` - The association cardinality
    * `field` - The name of the association field on the schema
    * `owner` - The schema where the association was defined
    * `related` - The schema that is associated
    * `owner_key` - The key on the `owner` schema used for the association
    * `queryable` - The real query to use for querying association
    * `on_delete` - The action taken on associations when schema is deleted
    * `on_replace` - The action taken on associations when schema is replaced
    * `defaults` - Default fields used when building the association
    * `relationship` - The relationship to the specified schema, default `:child`
    * `join_keys` - The keyword list with many to many join keys
    * `join_through` - Atom (representing a schema) or a string (representing a table)
      for many to many associations
    * `join_defaults` - A list of defaults for join associations
    * `preload_order` - Default `order_by` of the association, used only by preload
  """

  @doc false
  def __define__(mod, name, queryable, opts) do
    check!(:many_to_many, opts, "many_to_many/3")

    struct =
      association(mod, :many, name, Ecto.Association.ManyToMany, [queryable: queryable] ++ opts)

    Module.put_attribute(mod, :ecto_changeset_fields, {name, {:assoc, struct}})
  end

  @behaviour Ecto.Association
  @on_delete_opts [:nothing, :delete_all]
  @on_replace_opts [:raise, :mark_as_invalid, :delete]

  defstruct [
    :field,
    :owner,
    :related,
    :owner_key,
    :queryable,
    :on_delete,
    :on_replace,
    :join_keys,
    :join_through,
    :on_cast,
    where: [],
    join_where: [],
    defaults: [],
    join_defaults: [],
    relationship: :child,
    cardinality: :many,
    unique: false,
    ordered: false,
    preload_order: []
  ]

  @impl true
  def after_verify_validation(%{queryable: queryable, join_through: join_through}) do
    cond do
      not is_atom(queryable) ->
        :ok

      not Code.ensure_loaded?(queryable) ->
        {:error, "associated schema #{inspect(queryable)} does not exist"}

      not function_exported?(queryable, :__schema__, 2) ->
        {:error, "associated module #{inspect(queryable)} is not an Ecto schema"}

      not is_atom(join_through) ->
        :ok

      not Code.ensure_loaded?(join_through) ->
        {:error, ":join_through schema #{inspect(join_through)} does not exist"}

      not function_exported?(join_through, :__schema__, 2) ->
        {:error, ":join_through module #{inspect(join_through)} is not an Ecto schema"}

      true ->
        :ok
    end
  end

  @impl true
  def struct(module, name, opts) do
    opts = opts
    |> Keyword.put(:field, name)
    |> Keyword.put(:owner, module)
    |> Keyword.put_new(:on_delete, :nothing)
    |> Keyword.put_new(:on_replace, :raise)
    |> Keyword.put_new(:where, [])
    |> Keyword.put_new(:join_where, [])
    |> Keyword.put_new(:defaults, [])
    |> Keyword.put_new(:join_defaults, [])
    |> Keyword.put_new(:preload_order, [])

    queryable = Keyword.fetch!(opts, :queryable)
    related = Ecto.Association.related_from_query(queryable, name)

    join_keys = opts[:join_keys]
    join_through = opts[:join_through]
    validate_join_through(name, join_through)

    {owner_key, join_keys} =
      case join_keys do
        [{join_owner_key, owner_key}, {join_related_key, related_key}]
        when is_atom(join_owner_key) and is_atom(owner_key) and
               is_atom(join_related_key) and is_atom(related_key) ->
          {owner_key, join_keys}

        nil ->
          {:id, default_join_keys(module, related)}

        _ ->
          raise ArgumentError,
                "many_to_many #{inspect(name)} expect :join_keys to be a keyword list " <>
                  "with two entries, the first being how the join table should reach " <>
                  "the current schema and the second how the join table should reach " <>
                  "the associated schema. For example: #{inspect(default_join_keys(module, related))}"
      end

    unless Module.get_attribute(module, :ecto_fields)[owner_key] do
      raise ArgumentError,
            "schema does not have the field #{inspect(owner_key)} used by " <>
              "association #{inspect(name)}, please set the :join_keys option accordingly"
    end

    dbg(opts[:join_defaults])
    if(is_binary(join_through) and not is_nil(opts[:join_defaults]) and opts[:join_defaults] != []) do
      raise ArgumentError, ":join_defaults has no effect for a :join_through without a schema"
    end

    opts =  Enum.reduce(opts, opts, fn {option, _}, options -> Keyword.merge(options, opt_in(option, options, module, name)) end)

    %__MODULE__{
      field: opts[:field],
      cardinality: Keyword.fetch!(opts, :cardinality),
      owner: opts[:owner],
      related: related,
      owner_key: owner_key,
      queryable: queryable,
      on_delete: opts[:on_delete],
      on_replace: opts[:on_replace],
      defaults: opts[:defaults],
      where: opts[:where],
      preload_order: opts[:preload_order],
      join_keys: join_keys,
      join_where: opts[:join_where],
      join_through: join_through,
      join_defaults: opts[:join_defaults],
      unique: Keyword.get(opts, :unique, false)
    }
  end

  def opt_in(option, options, _module, name) when option in [:where, :join_where] do
    unless is_list(options[:where]) do
      raise ArgumentError,
            "expected `#{inspect(option)}` for #{inspect(name)} to be a keyword list, got: `#{inspect(options[:where])}`"
    end
    [{option, options[option]}]
    end

  def opt_in(:preload_order, options, _module, name) do
    preload_order = Ecto.Association.validate_preload_order!(name, options[:preload_order])
    [preload_order: preload_order]
  end

  def opt_in(option, options, module, name) when option in [:defaults, :join_defaults] do
    defaults = Ecto.Association.validate_defaults!(module, name, options[option])
    [{option, defaults}]

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
    on_replace = options[:on_replace]

    unless on_replace in @on_replace_opts do
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

  defp default_join_keys(module, related) do
    [
      {Ecto.Association.association_key(module, :id), :id},
      {Ecto.Association.association_key(related, :id), :id}
    ]
  end

  @impl true
  def joins_query(
        %{owner: owner, queryable: queryable, join_through: join_through, join_keys: join_keys} =
          assoc
      ) do
    [{join_owner_key, owner_key}, {join_related_key, related_key}] = join_keys

    from(o in owner,
      join: j in ^join_through,
      on: field(j, ^join_owner_key) == field(o, ^owner_key),
      join: q in ^queryable,
      on: field(j, ^join_related_key) == field(q, ^related_key)
    )
    |> Ecto.Association.combine_joins_query(assoc.where, 2)
    |> Ecto.Association.combine_joins_query(assoc.join_where, 1)
  end

  def assoc_query(%{queryable: queryable} = refl, values) do
    assoc_query(refl, queryable, values)
  end

  @impl true
  def assoc_query(assoc, query, values) do
    %{queryable: queryable, join_through: join_through, join_keys: join_keys, owner: owner} =
      assoc

    [{join_owner_key, owner_key}, {join_related_key, related_key}] = join_keys

    owner_key_type = owner.__schema__(:type, owner_key)

    # We only need to join in the "join table". Preload and Ecto.assoc expressions can then filter
    # by &1.join_owner_key in ^... to filter down to the associated entries in the related table.
    query =
      from(q in (query || queryable),
        join: j in ^join_through,
        on: field(q, ^related_key) == field(j, ^join_related_key),
        where: field(j, ^join_owner_key) in type(^values, {:in, ^owner_key_type})
      )
      |> Ecto.Association.combine_assoc_query(assoc.where)

    Ecto.Association.combine_joins_query(query, assoc.join_where, length(query.joins))
  end

  @impl true
  def build(refl, owner, attributes) do
    refl
    |> build(owner)
    |> struct(attributes)
  end

  @impl true
  def preload_info(%{join_keys: [{join_owner_key, owner_key}, {_, _}], owner: owner} = refl) do
    owner_key_type = owner.__schema__(:type, owner_key)

    # When preloading use the last bound table (which is the join table) and the join_owner_key
    # to filter out related entities to the owner structs we're preloading with.
    {:assoc, refl, {-1, join_owner_key, owner_key_type}}
  end

  @impl true
  def on_repo_change(
        %{on_replace: :delete} = refl,
        parent_changeset,
        %{action: :replace} = changeset,
        adapter,
        opts
      ) do
    on_repo_change(refl, parent_changeset, %{changeset | action: :delete}, adapter, opts)
  end

  def on_repo_change(
        %{join_keys: join_keys, join_through: join_through, join_where: join_where},
        %{repo: repo, data: owner},
        %{action: :delete, data: related},
        adapter,
        opts
      ) do
    [{join_owner_key, owner_key}, {join_related_key, related_key}] = join_keys
    owner_value = dump!(:delete, join_through, owner, owner_key, adapter)
    related_value = dump!(:delete, join_through, related, related_key, adapter)

    query =
      join_through
      |> where([j], field(j, ^join_owner_key) == ^owner_value)
      |> where([j], field(j, ^join_related_key) == ^related_value)
      |> Ecto.Association.combine_assoc_query(join_where)

    query = %{query | prefix: owner.__meta__.prefix}
    repo.delete_all(query, opts)
    {:ok, nil}
  end

  def on_repo_change(
        %{field: field, join_through: join_through, join_keys: join_keys} = refl,
        %{repo: repo, data: owner} = parent_changeset,
        %{action: action} = changeset,
        adapter,
        opts
      ) do
    changeset = Ecto.Association.update_parent_prefix(changeset, owner)

    case apply(repo, action, [changeset, opts]) do
      {:ok, related} ->
        [{join_owner_key, owner_key}, {join_related_key, related_key}] = join_keys

        if insert_join?(parent_changeset, changeset, field, related_key) do
          owner_value = dump!(:insert, join_through, owner, owner_key, adapter)
          related_value = dump!(:insert, join_through, related, related_key, adapter)
          data = %{join_owner_key => owner_value, join_related_key => related_value}

          case insert_join(join_through, refl, parent_changeset, data, opts) do
            {:error, join_changeset} ->
              {:error,
               %{
                 changeset
                 | errors: join_changeset.errors ++ changeset.errors,
                   valid?: join_changeset.valid? and changeset.valid?
               }}

            _ ->
              {:ok, related}
          end
        else
          {:ok, related}
        end

      {:error, changeset} ->
        {:error, changeset}
    end
  end

  defp validate_join_through(name, nil) do
    raise ArgumentError,
          "many_to_many #{inspect(name)} associations require the :join_through option to be given"
  end

  defp validate_join_through(_, join_through)
       when is_atom(join_through) or is_binary(join_through) do
    :ok
  end

  defp validate_join_through(name, _join_through) do
    raise ArgumentError,
          "many_to_many #{inspect(name)} associations require the :join_through option to be " <>
            "an atom (representing a schema) or a string (representing a table)"
  end

  defp insert_join?(%{action: :insert}, _, _field, _related_key), do: true
  defp insert_join?(_, %{action: :insert}, _field, _related_key), do: true

  defp insert_join?(%{data: owner}, %{data: related}, field, related_key) do
    current_key = Map.fetch!(related, related_key)

    not Enum.any?(Map.fetch!(owner, field), fn child ->
      Map.get(child, related_key) == current_key
    end)
  end

  defp insert_join(join_through, _refl, %{repo: repo, data: owner}, data, opts)
       when is_binary(join_through) do
    opts = Keyword.put_new(opts, :prefix, owner.__meta__.prefix)
    repo.insert_all(join_through, [data], opts)
  end

  defp insert_join(join_through, refl, parent_changeset, data, opts) when is_atom(join_through) do
    %{repo: repo, constraints: constraints, data: owner} = parent_changeset

    changeset =
      join_through
      |> Ecto.Association.apply_defaults(refl.join_defaults, owner)
      |> Map.merge(data)
      |> Ecto.Changeset.change()
      |> Map.put(:constraints, constraints)
      |> put_new_prefix(owner.__meta__.prefix)

    repo.insert(changeset, opts)
  end

  defp put_new_prefix(%{data: %{__meta__: %{prefix: prefix}}} = changeset, prefix),
    do: changeset

  defp put_new_prefix(%{data: %{__meta__: %{prefix: nil}}} = changeset, prefix),
    do: update_in(changeset.data, &Ecto.put_meta(&1, prefix: prefix))

  defp put_new_prefix(changeset, _),
    do: changeset

  defp field!(op, struct, field) do
    Map.get(struct, field) ||
      raise "could not #{op} join entry because `#{field}` is nil in #{inspect(struct)}"
  end

  defp dump!(action, join_through, struct, field, adapter) when is_binary(join_through) do
    value = field!(action, struct, field)
    type = struct.__struct__.__schema__(:type, field)

    case Ecto.Type.adapter_dump(adapter, type, value) do
      {:ok, value} ->
        value

      :error ->
        raise Ecto.ChangeError,
              "value `#{inspect(value)}` for `#{inspect(struct.__struct__)}.#{field}` " <>
                "in `#{action}` does not match type #{Ecto.Type.format(type)}"
    end
  end

  defp dump!(action, join_through, struct, field, _) when is_atom(join_through) do
    field!(action, struct, field)
  end

  ## On delete callbacks

  @doc false
  def delete_all(refl, parent, repo_name, opts) do
    %{join_through: join_through, join_keys: join_keys, owner: owner} = refl
    [{join_owner_key, owner_key}, {_, _}] = join_keys

    if value = Map.get(parent, owner_key) do
      owner_type = owner.__schema__(:type, owner_key)

      query =
        from j in join_through, where: field(j, ^join_owner_key) == type(^value, ^owner_type)

      Ecto.Repo.Queryable.delete_all(repo_name, query, opts)
    end
  end
end
