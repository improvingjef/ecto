defmodule Ecto.Association.Options do
  @default_options %{
    field: [
      :default,
      :source,
      :autogenerate,
      :read_after_writes,
      :virtual,
      :primary_key,
      :load_in_query,
      :redact,
      :foreign_key,
      :on_replace,
      :defaults,
      :type,
      :where,
      :references,
      :skip_default_validation,
      :writable
    ],
    belongs_to: [
      :foreign_key,
      :references,
      :define_field,
      :type,
      :on_replace,
      :defaults,
      :primary_key,
      :source,
      :where
    ],
    has: [
      :foreign_key,
      :references,
      :through,
      :on_delete,
      :defaults,
      :on_replace,
      :where,
      :preload_order
    ],
    many_to_many: [
      :join_through,
      :join_defaults,
      :join_keys,
      :on_delete,
      :defaults,
      :on_replace,
      :unique,
      :where,
      :join_where,
      :preload_order
    ],
    embeds_one: [
      :on_replace,
      :source,
      :load_in_query,
      :defaults_to_struct
    ],
    embeds_many: [
      :on_replace,
      :source,
      :load_in_query
    ]
  }

  defp valid(which) do
    Map.get(@default_options, which, [])
  end

  def check!(which, opts, fun_arity) do
    case Enum.find(opts, fn {k, _} -> k not in valid(which) end) do
      {k, _} -> raise ArgumentError, "invalid option #{inspect(k)} for #{fun_arity}"
      nil -> :ok
    end
  end

  def check!(_which, {:parameterized, _}, _opts, _fun_arity) do
    :ok
  end

  def check!(which, {_, type}, opts, fun_arity) do
    check!(which, type, opts, fun_arity)
  end

  def check!(which, _type, opts, fun_arity) do
    check!(which, opts, fun_arity)
  end

  # Internal function for integrating associations into schemas.
  #
  # This function exists as an extension point for libraries to
  # experiment new types of associations to Ecto, although it may
  # break at any time (as with any of the association callbacks).
  #
  # This function expects the current schema, the association cardinality,
  # the association name, the association module (that implements
  # `Ecto.Association` callbacks) and a keyword list of options.
  @doc false
  @spec association(module, :one | :many, atom(), module, Keyword.t()) :: Ecto.Association.t()
  def association(schema, cardinality, name, association, opts) do
    not_loaded = %Ecto.Association.NotLoaded{
      __owner__: schema,
      __field__: name,
      __cardinality__: cardinality
    }

    put_struct_field(schema, name, not_loaded)
    opts = [cardinality: cardinality] ++ opts
    struct = association.struct(schema, name, opts)
    Module.put_attribute(schema, :ecto_assocs, {name, struct})
    struct
  end

  # PRIVATE

  def put_struct_field(mod, name, assoc) do
    fields = Module.get_attribute(mod, :ecto_struct_fields)

    if List.keyfind(fields, name, 0) do
      raise ArgumentError,
            "field/association #{inspect(name)} already exists on schema, you must either remove the duplication or choose a different name"
    end

    Module.put_attribute(mod, :ecto_struct_fields, {name, assoc})
  end
end
