defmodule Ecto.Association.HasThrough do
  import Ecto.Query, only: [from: 1]

  @moduledoc """
  The association struct for `has_one` and `has_many` through associations.

  Its fields are:

    * `cardinality` - The association cardinality
    * `field` - The name of the association field on the schema
    * `owner` - The schema where the association was defined
    * `owner_key` - The key on the `owner` schema used for the association
    * `through` - The through associations
    * `relationship` - The relationship to the specified schema, default `:child`
  """

  @behaviour Ecto.Association
  defstruct [
    :cardinality,
    :field,
    :owner,
    :owner_key,
    :through,
    :on_cast,
    relationship: :child,
    unique: true,
    ordered: false
  ]

  @impl true
  def after_verify_validation(_) do
    :ok
  end

  @impl true
  def struct(module, name, opts) do
    through = Keyword.fetch!(opts, :through)

    refl =
      case through do
        [h, _ | _] ->
          Module.get_attribute(module, :ecto_assocs)[h]

        _ ->
          raise ArgumentError,
                ":through expects a list with at least two entries: " <>
                  "the association in the current module and one step through, got: #{inspect(through)}"
      end

    unless refl do
      raise ArgumentError,
            "schema does not have the association #{inspect(hd(through))} " <>
              "used by association #{inspect(name)}, please ensure the association exists and " <>
              "is defined before the :through one"
    end

    %__MODULE__{
      field: name,
      cardinality: Keyword.fetch!(opts, :cardinality),
      through: through,
      owner: module,
      owner_key: refl.owner_key
    }
  end

  @impl true
  def build(%{field: name}, %{__struct__: owner}, _attributes) do
    raise ArgumentError,
          "cannot build through association `#{inspect(name)}` for #{inspect(owner)}. " <>
            "Instead build the intermediate steps explicitly."
  end

  @impl true
  def preload_info(%{through: through} = refl) do
    {:through, refl, through}
  end

  @impl true
  def on_repo_change(%{field: name}, _, _, _, _) do
    raise ArgumentError,
          "cannot insert/update/delete through associations `#{inspect(name)}` via the repository. " <>
            "Instead build the intermediate steps explicitly."
  end

  @impl true
  def joins_query(%{owner: owner, through: through}) do
    Ecto.Association.join_through_chain(owner, through, from(x in owner))
  end

  @impl true
  def assoc_query(%{owner: owner, through: through}, _, values) do
    Ecto.Association.filter_through_chain(owner, through, values)
  end
end
