defmodule Ecto.Schema.Embeds do
  import Ecto.Association.CheckOptions, only: [check_options!: 3]

  @valid_embeds_one_options [:on_replace, :source, :load_in_query, :defaults_to_struct]

  @doc false
  def __embeds_one__(mod, name, schema, opts) when is_atom(schema) do
    check_options!(opts, @valid_embeds_one_options, "embeds_one/3")

    opts =
      if Keyword.get(opts, :defaults_to_struct) do
        Keyword.put(opts, :default, schema.__schema__(:loaded))
      else
        opts
      end

    embed(mod, :one, name, schema, opts)
  end

  def __embeds_one__(_mod, _name, schema, _opts) do
    raise ArgumentError,
          "`embeds_one/3` expects `schema` to be a module name, but received #{inspect(schema)}"
  end

  @valid_embeds_many_options [:on_replace, :source, :load_in_query]

  @doc false
  def __embeds_many__(mod, name, schema, opts) when is_atom(schema) do
    check_options!(opts, @valid_embeds_many_options, "embeds_many/3")
    opts = Keyword.put(opts, :default, [])
    embed(mod, :many, name, schema, opts)
  end

  def __embeds_many__(_mod, _name, schema, _opts) do
    raise ArgumentError,
          "`embeds_many/3` expects `schema` to be a module name, but received #{inspect(schema)}"
  end

  defp embed(mod, cardinality, name, schema, opts) do
    opts = [cardinality: cardinality, related: schema, owner: mod, field: name] ++ opts
    struct = Ecto.Embedded.init(opts)

    Module.put_attribute(mod, :ecto_changeset_fields, {name, {:embed, struct}})
    Module.put_attribute(mod, :ecto_embeds, {name, struct})
    Ecto.Schema.Field.define_field(mod, name, {:parameterized, {Ecto.Embedded, struct}}, opts)
  end
end
