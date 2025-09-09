defmodule Ecto.Schema.Embeds do
  import Ecto.Association.Options, only: [check!: 3]

  @doc false
  def __define__(mod, :one, name, schema, opts, fun_arity) when is_atom(schema) do
    check!(:embeds_one, opts, fun_arity)

    opts =
      if Keyword.get(opts, :defaults_to_struct) do
        Keyword.put(opts, :default, schema.__schema__(:loaded))
      else
        opts
      end

    embed(mod, :one, name, schema, opts)
  end

  @doc false
  def __define__(mod, :many, name, schema, opts, fun_arity) when is_atom(schema) do
    check!(:embeds_many, opts, fun_arity)
    opts = Keyword.put(opts, :default, [])
    embed(mod, :many, name, schema, opts)
  end

  def __define__(_mod, _cardinality, _name, schema, _opts, fun_arity) do
    raise ArgumentError,
          "`#{fun_arity}` expects `schema` to be a module name, but received #{inspect(schema)}"
  end

  defp embed(mod, cardinality, name, schema, opts) do
    opts = [cardinality: cardinality, related: schema, owner: mod, field: name] ++ opts
    struct = Ecto.Embedded.init(opts)

    Module.put_attribute(mod, :ecto_changeset_fields, {name, {:embed, struct}})
    Module.put_attribute(mod, :ecto_embeds, {name, struct})
    Ecto.Schema.Field.define_field(mod, name, {:parameterized, {Ecto.Embedded, struct}}, opts)
  end
end
