defmodule Ecto.Association.Options do
  def check!(opts, valid, fun_arity) do
    case Enum.find(opts, fn {k, _} -> k not in valid end) do
      {k, _} -> raise ArgumentError, "invalid option #{inspect(k)} for #{fun_arity}"
      nil -> :ok
    end
  end

  def check!({:parameterized, _}, _opts, _valid, _fun_arity) do
    :ok
  end

  def check!({_, type}, opts, valid, fun_arity) do
    check!(type, opts, valid, fun_arity)
  end

  def check!(_type, opts, valid, fun_arity) do
    check!(opts, valid, fun_arity)
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
