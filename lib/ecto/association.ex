import Ecto.Query, only: [from: 1, join: 4, join: 5, distinct: 3, where: 3]

defmodule Ecto.Association do
  @moduledoc false

  @type t :: %{
          required(:__struct__) => atom,
          required(:on_cast) => nil | fun,
          required(:cardinality) => :one | :many,
          required(:relationship) => :parent | :child,
          required(:owner) => atom,
          required(:owner_key) => atom,
          required(:field) => atom,
          required(:unique) => boolean,
          optional(atom) => any
        }

  alias Ecto.Query.Builder.OrderBy

  @doc """
  Builds the association struct.

  The struct must be defined in the module that implements the
  callback and it must contain at least the following keys:

    * `:cardinality` - tells if the association is one to one
      or one/many to many

    * `:field` - tells the field in the owner struct where the
      association should be stored

    * `:owner` - the owner module of the association

    * `:owner_key` - the key in the owner with the association value

    * `:relationship` - if the relationship to the specified schema is
      of a `:child` or a `:parent`

  """
  @callback struct(module, field :: atom, opts :: Keyword.t()) :: t

  @doc """
  Invoked after the schema is compiled to validate associations.

  Useful for checking if associated modules exist without running
  into deadlocks.
  """
  @callback after_verify_validation(t) :: :ok | {:error, String.t()}

  @doc """
  Builds a struct for the given association.

  The struct to build from is given as argument in case default values
  should be set in the struct.

  Invoked by `Ecto.build_assoc/3`.
  """
  @callback build(t, owner :: Ecto.Schema.t(), %{atom => term} | [Keyword.t()]) :: Ecto.Schema.t()

  @doc """
  Returns an association join query.

  This callback receives the association struct and it must return
  a query that retrieves all associated entries using joins up to
  the owner association.

  For example, a `has_many :comments` inside a `Post` module would
  return:

      from c in Comment, join: p in Post, on: c.post_id == p.id

  Note all the logic must be expressed inside joins, as fields like
  `where` and `order_by` won't be used by the caller.

  This callback is invoked when `join: assoc(p, :comments)` is used
  inside queries.
  """
  @callback joins_query(t) :: Ecto.Query.t()

  @doc """
  Returns the association query on top of the given query.

  If the query is `nil`, the association target must be used.

  This callback receives the association struct and it must return
  a query that retrieves all associated entries with the given
  values for the owner key.

  This callback is used by `Ecto.assoc/2` and when preloading.
  """
  @callback assoc_query(t, Ecto.Query.t() | nil, values :: [term]) :: Ecto.Query.t()

  @doc """
  Returns information used by the preloader.
  """
  @callback preload_info(t) ::
              {:assoc, t, {integer, atom} | {integer, atom, Ecto.Type.t()}}
              | {:through, t, [atom]}

  @doc """
  Performs the repository change on the association.

  Receives the parent changeset, the current changesets
  and the repository action options. Must return the
  persisted struct (or nil) or the changeset error.
  """
  @callback on_repo_change(
              t,
              parent :: Ecto.Changeset.t(),
              changeset :: Ecto.Changeset.t(),
              Ecto.Adapter.t(),
              Keyword.t()
            ) ::
              {:ok, Ecto.Schema.t() | nil} | {:error, Ecto.Changeset.t()}

  @doc """
  Retrieves the association from the given schema.
  """
  def association_from_schema!(schema, assoc) do
    schema.__schema__(:association, assoc) ||
      raise ArgumentError, "schema #{inspect(schema)} does not have association #{inspect(assoc)}"
  end

  @doc """
  Returns the association key for the given module with the given suffix.

  ## Examples

      iex> Ecto.Association.association_key(Hello.World, :id)
      :world_id

      iex> Ecto.Association.association_key(Hello.HTTP, :id)
      :http_id

      iex> Ecto.Association.association_key(Hello.HTTPServer, :id)
      :http_server_id

  """
  def association_key(module, suffix) do
    prefix = module |> Module.split() |> List.last() |> Macro.underscore()
    :"#{prefix}_#{suffix}"
  end

  @doc """
  Build an association query through the given associations from the specified owner table
  and through the given associations. Finally filter by the provided values of the owner_key of
  the first relationship in the chain. Used in Ecto.assoc/2.
  """
  def filter_through_chain(owner, through, values) do
    chain_through(owner, through, nil, values)
    |> distinct([x], true)
  end

  @doc """
  Join the target table given a list of associations to go through starting from the owner table.
  """
  def join_through_chain(owner, through, query) do
    chain_through(owner, through, query, nil)
  end

  # This function is used by both join_through_chain/3 and filter_through_chain/3 since the algorithm for both
  # is nearly identical barring a few differences.
  defp chain_through(owner, through, join_to, values) do
    # Flatten the chain of throughs. If any of the associations is a HasThrough this allows us to expand it so we have
    # a list of atomic associations to join through.
    {_, through} = flatten_through_chain(owner, through, [])

    # If we're joining then we're going forward from the owner table to the destination table.
    # Otherwise we're going backward from the destination table then filtering by values.
    chain_direction = if(join_to != nil, do: :forward, else: :backward)

    # This stage produces a list of joins represented as a keyword list with the following structure:
    # [
    #   [schema: (The Schema), in_key: (The key used to join into the table), out_key: (The key used to join with the next), where: (The condition KW list)]
    # ]
    relation_list = resolve_through_tables(owner, through, chain_direction)

    # Filter out the joins which are redundant
    filtered_list =
      Enum.with_index(relation_list)
      |> Enum.filter(fn
        # We always keep the first table in the chain since it's our source table for the query
        {_, 0} ->
          true

        {rel, _} ->
          # If the condition is not empty we need to join to the table. Otherwise if the in_key and out_key is the same
          # then this join is redundant since we can just join to the next table in the chain.
          rel.in_key != rel.out_key or rel.where != []
      end)
      |> Enum.map(&elem(&1, 0))

    # If we're preloading we don't need the last table since it is the owner table.
    filtered_list = if(join_to == nil, do: Enum.drop(filtered_list, -1), else: filtered_list)

    [source | joins] = filtered_list

    source_schema = source.schema
    query = join_to || from(s in source_schema)

    counter = Ecto.Query.Builder.count_binds(query) - 1

    # We need to create the query by joining all the tables, and also we need the out_key of the final table to use
    # for the final WHERE clause with values.
    {_, query, _, dest_out_key} =
      Enum.reduce(joins, {source, query, counter, source.out_key}, fn
        curr_rel, {prev_rel, query, counter, _} ->
          related_queryable = curr_rel.schema

          next =
            join(query, :inner, [{src, counter}], dest in ^related_queryable,
              on: field(src, ^prev_rel.out_key) == field(dest, ^curr_rel.in_key)
            )
            |> combine_joins_query(curr_rel.where, counter + 1)

          {curr_rel, next, counter + 1, curr_rel.out_key}
      end)

    final_bind = Ecto.Query.Builder.count_binds(query) - 1

    values = List.wrap(values)

    query =
      case {join_to, values} do
        {nil, [single_value]} ->
          where(query, [{dest, final_bind}], field(dest, ^dest_out_key) == ^single_value)

        {nil, values} ->
          where(query, [{dest, final_bind}], field(dest, ^dest_out_key) in ^values)

        {_, _} ->
          query
      end

    combine_assoc_query(query, source.where || [])
  end

  defp flatten_through_chain(owner, [], acc), do: {owner, acc}

  defp flatten_through_chain(owner, [assoc | tl], acc) do
    refl = association_from_schema!(owner, assoc)

    case refl do
      %{through: nested_throughs} ->
        {owner, acc} = flatten_through_chain(owner, nested_throughs, acc)
        flatten_through_chain(owner, tl, acc)

      _ ->
        flatten_through_chain(refl.related, tl, acc ++ [assoc])
    end
  end

  defp resolve_through_tables(owner, through, :backward) do
    # This step generates a list of maps with the following keys:
    # [
    #   %{schema: ..., out_key: ..., in_key: ..., where: ...}
    # ]
    # This is a list of all tables that we will need to join to follow the chain of throughs and which key is used
    # to join in and out of the table, along with the where condition for that table. The final table of the chain will
    # be "owner", and the first table of the chain will be the final destination table of all the throughs.
    initial_owner_map = %{schema: owner, out_key: nil, in_key: nil, where: nil}

    Enum.reduce(through, {owner, [initial_owner_map]}, fn assoc, {owner, table_list} ->
      refl = association_from_schema!(owner, assoc)
      [owner_map | table_list] = table_list

      table_list =
        case refl do
          %{
            join_through: join_through,
            join_keys: join_keys,
            join_where: join_where,
            where: where
          } ->
            [{owner_join_key, owner_key}, {related_join_key, related_key}] = join_keys

            owner_map = %{owner_map | in_key: owner_key}

            join_map = %{
              schema: join_through,
              out_key: owner_join_key,
              in_key: related_join_key,
              where: join_where
            }

            related_map = %{schema: refl.related, out_key: related_key, in_key: nil, where: where}

            [related_map, join_map, owner_map | table_list]

          _ ->
            owner_map = %{owner_map | in_key: refl.owner_key}

            related_map = %{
              schema: refl.related,
              out_key: refl.related_key,
              in_key: nil,
              where: refl.where
            }

            [related_map, owner_map | table_list]
        end

      {refl.related, table_list}
    end)
    |> elem(1)
  end

  defp resolve_through_tables(owner, through, :forward) do
    # In the forward case (joining) we need to reverse the list and swap the in_key for the out_key
    # since we've changed directions.
    resolve_through_tables(owner, through, :backward)
    |> Enum.reverse()
    |> Enum.map(fn %{out_key: out_key, in_key: in_key} = join ->
      %{join | out_key: in_key, in_key: out_key}
    end)
  end

  @doc """
  Add the default assoc query where clauses to a join.

  This handles only `where` and converts it to a `join`,
  as that is the only information propagate in join queries.
  """
  def combine_joins_query(query, [], _binding), do: query

  def combine_joins_query(%{joins: joins} = query, [_ | _] = conditions, binding) do
    {joins, [join_expr]} = Enum.split(joins, -1)
    %{on: %{params: params, expr: expr} = join_on} = join_expr
    {expr, params} = expand_where(conditions, expr, Enum.reverse(params), length(params), binding)
    %{query | joins: joins ++ [%{join_expr | on: %{join_on | expr: expr, params: params}}]}
  end

  @doc """
  Add the default assoc query where clauses a provided query.
  """
  def combine_assoc_query(query, []), do: query

  def combine_assoc_query(%{wheres: []} = query, conditions) do
    {expr, params} = expand_where(conditions, true, [], 0, 0)

    bool_expr = %Ecto.Query.BooleanExpr{
      op: :and,
      expr: expr,
      params: params,
      line: __ENV__.line,
      file: __ENV__.file
    }

    %{query | wheres: [bool_expr]}
  end

  def combine_assoc_query(%{wheres: wheres} = query, conditions) do
    {wheres, [where_expr]} = Enum.split(wheres, -1)
    %{params: params, expr: expr} = where_expr
    {expr, params} = expand_where(conditions, expr, Enum.reverse(params), length(params), 0)
    %{query | wheres: wheres ++ [%{where_expr | expr: expr, params: params}]}
  end

  defp expand_where(conditions, expr, params, counter, binding) do
    conjoin_exprs = fn
      true, r -> r
      l, r -> {:and, [], [l, r]}
    end

    {expr, params, _counter} =
      Enum.reduce(conditions, {expr, params, counter}, fn
        {key, nil}, {expr, params, counter} ->
          expr = conjoin_exprs.(expr, {:is_nil, [], [to_field(binding, key)]})
          {expr, params, counter}

        {key, {:not, nil}}, {expr, params, counter} ->
          expr = conjoin_exprs.(expr, {:not, [], [{:is_nil, [], [to_field(binding, key)]}]})
          {expr, params, counter}

        {key, {:fragment, frag}}, {expr, params, counter} when is_binary(frag) ->
          pieces = Ecto.Query.Builder.fragment_pieces(frag, [to_field(binding, key)])
          expr = conjoin_exprs.(expr, {:fragment, [], pieces})
          {expr, params, counter}

        {key, {:in, value}}, {expr, params, counter} when is_list(value) ->
          expr = conjoin_exprs.(expr, {:in, [], [to_field(binding, key), {:^, [], [counter]}]})
          {expr, [{value, {:in, {binding, key}}} | params], counter + 1}

        {key, value}, {expr, params, counter} ->
          expr = conjoin_exprs.(expr, {:==, [], [to_field(binding, key), {:^, [], [counter]}]})
          {expr, [{value, {binding, key}} | params], counter + 1}
      end)

    {expr, Enum.reverse(params)}
  end

  defp to_field(binding, field),
    do: {{:., [], [{:&, [], [binding]}, field]}, [], []}

  @doc """
  Build a join query with the given `through` associations starting at `counter`.
  """
  def joins_query(query, through, counter) do
    Enum.reduce(through, {query, counter}, fn current, {acc, counter} ->
      query = join(acc, :inner, [{x, counter}], assoc(x, ^current))
      {query, counter + 1}
    end)
    |> elem(0)
  end

  @doc """
  Retrieves related module from queryable.

  ## Examples

      iex> Ecto.Association.related_from_query({"custom_source", Schema}, :comments_v1)
      Schema

      iex> Ecto.Association.related_from_query(Schema, :comments_v1)
      Schema

      iex> Ecto.Association.related_from_query("wrong", :comments_v1)
      ** (ArgumentError) association :comments_v1 queryable must be a schema or a {source, schema}. got: "wrong"
  """
  def related_from_query(atom, _name) when is_atom(atom), do: atom

  def related_from_query({source, schema}, _name) when is_binary(source) and is_atom(schema),
    do: schema

  def related_from_query(queryable, name) do
    raise ArgumentError,
          "association #{inspect(name)} queryable must be a schema or " <>
            "a {source, schema}. got: #{inspect(queryable)}"
  end

  @doc """
  Applies default values into the struct.
  """
  def apply_defaults(struct, defaults, _owner) when is_list(defaults) do
    struct(struct, defaults)
  end

  def apply_defaults(struct, {mod, fun, args}, owner) do
    apply(mod, fun, [struct.__struct__(), owner | args])
  end

  @doc """
  Validates `defaults` for association named `name`.
  """
  def validate_defaults!(_module, _name, {mod, fun, args} = defaults)
      when is_atom(mod) and is_atom(fun) and is_list(args),
      do: defaults

  def validate_defaults!(module, _name, fun) when is_atom(fun),
    do: {module, fun, []}

  def validate_defaults!(_module, _name, defaults) when is_list(defaults),
    do: defaults

  def validate_defaults!(_module, name, defaults) do
    raise ArgumentError,
          "expected defaults for #{inspect(name)} to be a keyword list " <>
            "or a {module, fun, args} tuple, got: `#{inspect(defaults)}`"
  end

  @doc """
  Validates `preload_order` for association named `name`.
  """
  def validate_preload_order!(_name, {mod, fun, args} = preload_order)
      when is_atom(mod) and is_atom(fun) and is_list(args),
      do: preload_order

  def validate_preload_order!(name, preload_order) when is_list(preload_order) do
    Enum.map(preload_order, fn
      field when is_atom(field) ->
        field

      {direction, field} when is_atom(direction) and is_atom(field) ->
        unless OrderBy.valid_direction?(direction) do
          raise ArgumentError,
                "expected `:preload_order` for #{inspect(name)} to be a keyword list or a list of atoms/fields, " <>
                  "got: `#{inspect(preload_order)}`, " <>
                  "`#{inspect(direction)}` is not a valid direction"
        end

        {direction, field}

      item ->
        raise ArgumentError,
              "expected `:preload_order` for #{inspect(name)} to be a keyword list or a list of atoms/fields, " <>
                "got: `#{inspect(preload_order)}`, " <>
                "`#{inspect(item)}` is not valid"
    end)
  end

  def validate_preload_order!(name, preload_order) do
    raise ArgumentError,
          "expected `:preload_order` for #{inspect(name)} to be a keyword list, a list of atoms/fields " <>
            "or a {Mod, fun, args} tuple, got: `#{inspect(preload_order)}`"
  end

  @doc """
  Merges source from query into to the given schema.

  In case the query does not have a source, returns
  the schema unchanged.
  """
  def merge_source(schema, query)

  def merge_source(%{__meta__: %{source: source}} = struct, {source, _}) do
    struct
  end

  def merge_source(struct, {source, _}) do
    Ecto.put_meta(struct, source: source)
  end

  def merge_source(struct, _query) do
    struct
  end

  @doc """
  Updates the prefix of a changeset based on the metadata.
  """
  def update_parent_prefix(
        %{data: %{__meta__: %{prefix: prefix}}} = changeset,
        %{__meta__: %{prefix: prefix}}
      ),
      do: changeset

  def update_parent_prefix(
        %{data: %{__meta__: %{prefix: nil}}} = changeset,
        %{__meta__: %{prefix: prefix}}
      ),
      do: update_in(changeset.data, &Ecto.put_meta(&1, prefix: prefix))

  def update_parent_prefix(changeset, _),
    do: changeset

  @doc """
  Performs the repository action in the related changeset,
  returning `{:ok, data}` or `{:error, changes}`.
  """
  def on_repo_change(%{data: struct}, [], _adapter, _opts) do
    {:ok, struct}
  end

  def on_repo_change(changeset, assocs, adapter, opts) do
    %{data: struct, changes: changes, action: action} = changeset

    {struct, changes, _halt, valid?} =
      Enum.reduce(assocs, {struct, changes, false, true}, fn {refl, value}, acc ->
        on_repo_change(refl, value, changeset, action, adapter, opts, acc)
      end)

    case valid? do
      true -> {:ok, struct}
      false -> {:error, changes}
    end
  end

  defp on_repo_change(
         %{cardinality: :one, field: field} = meta,
         nil,
         parent_changeset,
         _repo_action,
         adapter,
         opts,
         {parent, changes, halt, valid?}
       ) do
    if not halt, do: maybe_replace_one!(meta, nil, parent, parent_changeset, adapter, opts)
    {Map.put(parent, field, nil), Map.put(changes, field, nil), halt, valid?}
  end

  defp on_repo_change(
         %{cardinality: :one, field: field, __struct__: mod} = meta,
         %{action: action, data: current} = changeset,
         parent_changeset,
         repo_action,
         adapter,
         opts,
         {parent, changes, halt, valid?}
       ) do
    check_action!(meta, action, repo_action)
    if not halt, do: maybe_replace_one!(meta, current, parent, parent_changeset, adapter, opts)

    case on_repo_change_unless_halted(halt, mod, meta, parent_changeset, changeset, adapter, opts) do
      {:ok, struct} ->
        {Map.put(parent, field, struct), Map.put(changes, field, changeset), halt, valid?}

      {:error, error_changeset} ->
        {parent, Map.put(changes, field, error_changeset),
         halted?(halt, changeset, error_changeset), false}
    end
  end

  defp on_repo_change(
         %{cardinality: :many, field: field, __struct__: mod} = meta,
         changesets,
         parent_changeset,
         repo_action,
         adapter,
         opts,
         {parent, changes, halt, all_valid?}
       ) do
    {changesets, structs, halt, valid?} =
      Enum.reduce(changesets, {[], [], halt, true}, fn
        %{action: action} = changeset, {changesets, structs, halt, valid?} ->
          check_action!(meta, action, repo_action)

          case on_repo_change_unless_halted(
                 halt,
                 mod,
                 meta,
                 parent_changeset,
                 changeset,
                 adapter,
                 opts
               ) do
            {:ok, nil} ->
              {[changeset | changesets], structs, halt, valid?}

            {:ok, struct} ->
              {[changeset | changesets], [struct | structs], halt, valid?}

            {:error, error_changeset} ->
              {[error_changeset | changesets], structs, halted?(halt, changeset, error_changeset),
               false}
          end
      end)

    if valid? do
      {Map.put(parent, field, Enum.reverse(structs)),
       Map.put(changes, field, Enum.reverse(changesets)), halt, all_valid?}
    else
      {parent, Map.put(changes, field, Enum.reverse(changesets)), halt, false}
    end
  end

  defp check_action!(%{related: schema}, :delete, :insert) do
    raise ArgumentError,
          "got action :delete in changeset for associated #{inspect(schema)} while inserting"
  end

  defp check_action!(_, _, _), do: :ok

  defp halted?(true, _, _), do: true
  defp halted?(_, %{valid?: true}, %{valid?: false}), do: true
  defp halted?(_, _, _), do: false

  defp on_repo_change_unless_halted(true, _mod, _meta, _parent, changeset, _adapter, _opts) do
    {:error, changeset}
  end

  defp on_repo_change_unless_halted(false, mod, meta, parent, changeset, adapter, opts) do
    mod.on_repo_change(meta, parent, changeset, adapter, opts)
  end

  defp maybe_replace_one!(
         %{field: field, __struct__: mod} = meta,
         current,
         parent,
         parent_changeset,
         adapter,
         opts
       ) do
    previous = Map.get(parent, field)

    if replaceable?(previous) and primary_key!(previous) != primary_key!(current) do
      changeset = %{Ecto.Changeset.change(previous) | action: :replace}

      case mod.on_repo_change(meta, parent_changeset, changeset, adapter, opts) do
        {:ok, _} ->
          :ok

        {:error, changeset} ->
          raise Ecto.InvalidChangesetError, action: changeset.action, changeset: changeset
      end
    end
  end

  defp maybe_replace_one!(_, _, _, _, _, _), do: :ok

  defp replaceable?(nil), do: false
  defp replaceable?(%Ecto.Association.NotLoaded{}), do: false
  defp replaceable?(%{__meta__: %{state: :built}}), do: false
  defp replaceable?(_), do: true

  defp primary_key!(nil), do: []
  defp primary_key!(struct), do: Ecto.primary_key!(struct)
end
