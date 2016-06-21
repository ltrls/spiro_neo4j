defmodule Spiro.Adapter.Neo4j do
  #@behaviour Spiro.Adapter
  use GenServer
  require Logger

  alias Spiro.Vertex
  alias Spiro.Edge

  def start_link(opts, module) do
    opts = opts || []
    GenServer.start_link(__MODULE__, opts, [name: module])
  end

  def add_vertex(v, module) do
    GenServer.call(module, {:add_vertex, v})
  end

  def add_edge(e, module) do
    GenServer.call(module, {:add_edge, e})
  end

  defp vertex_url(id, opts), do: opts[:host] <> "/db/data/node/#{id}"
  defp edge_url(id, opts), do: opts[:host] <> "/db/data/relationship/#{id}"
  defp get_id(url), do: Regex.run(~r/\d+$/, url) |> List.first |> String.to_integer
  defp create_edge(edge) do
    %Edge{id: edge["metadata"]["id"],
      type: edge["type"],
      properties: edge["data"],
      to: %Vertex{id: get_id(edge["start"])},
      from: %Vertex{id: get_id(edge["en" <> "d"])} }
  end
  defp create_vertex(vertex) do
    %Vertex{id: vertex["metadata"]["id"],
      labels: vertex["metadata"]["labels"],
      properties: vertex["data"] }
  end

  defp request(method, url, params, opts) do
    url = opts[:host] <> url
    headers = [{"Accept", "application/json"},
      {"Accept", "application/json; charset=UTF-8"},
      {"Content-Type", "application/json; charset=UTF-8"},
      {"User-Agent", "Spiro"},
      {"X-Stream", "true"}]
    hackney = [basic_auth: {opts[:username], opts[:password]}]
    Logger.info params
    case HTTPoison.request!(method, url, Poison.encode!(params), headers, [hackney: hackney]) do
      %HTTPoison.Response{status_code: 204} -> :no_content
      %HTTPoison.Response{status_code: 200, body: body} -> {:ok, Poison.decode!(body)}
      %HTTPoison.Response{status_code: 201, body: body} -> {:created, Poison.decode!(body)}
      %HTTPoison.Response{status_code: 400, body: body} -> {:error, Poison.decode!(body)["exception"]}
      %HTTPoison.Response{status_code: 404, body: body} -> {:not_found, Poison.decode!(body)["exception"]}
      %HTTPoison.Response{status_code: 409, body: body} -> {:conflict, Poison.decode!(body)["exception"]}
    end
  end

  # server callbacks
  def handle_call({:add_vertex, %Vertex{properties: properties} = vertex}, _from, opts) do
    params = properties |> Enum.into(%{})
    case request(:post, "/db/data/node", params, opts) do
      {:created, v} -> {:reply, %{vertex | id: v["metadata"]["id"]}, opts}
      {:error, "PropertyValueException"} -> {:reply, {:error, :bad_input}}
    end
  end

  def handle_call({:add_edge, %Edge{properties: properties, to: to, from: from, type: type} = edge}, _from, opts) do
    url = "/db/data/node/#{from.id}/relationships"
    params = %{to: vertex_url(to.id, opts),
      type: type,
      data: Enum.into(properties, %{}) }
    case request(:post, url, params, opts) do
      {:created, e} -> {:reply, %{edge | id: e["metadata"]["id"]}, opts}
      {:error, "PropertyValueException"} -> {:reply, {:error, :bad_input}}
    end
  end

  def handle_call({:update_vertex, %Vertex{id: id, properties: properties} = vertex}, _from, opts) do
    params = properties |> Enum.into(%{})
    case request(:put, vertex_url(id, opts) <> "/properties", params, opts) do
      :no_content -> {:reply, vertex, opts}
      {:not_found, "NodeNotFoundException"} -> {:reply, {:error, :not_found}, opts}
      {:error, "PropertyValueException"} -> {:reply, {:error, :bad_input}}
    end
  end

  def handle_call({:update_edge, %Edge{id: id, properties: properties} = edge}, _from, opts) do
    params = properties |> Enum.into(%{})
    case request(:put, edge_url(id, opts) <> "/properties", params, opts) do
      :no_content ->{:reply, edge, opts}
      {:not_found, "RelationshipNotFoundException"} -> {:reply, {:error, :not_found}, opts}
      {:error, "PropertyValueException"} -> {:reply, {:error, :bad_input}}
    end
  end

  def handle_call({:delete_vertex, %Vertex{id: id} = vertex}, _from, opts) do
    case request(:delete, vertex_url(id, opts), %{}, opts) do
      :no_content -> {:noreply, opts}
      {:not_found, "NodeNotFoundException"} -> {:reply, {:error, :not_found}, opts}
      {:conflict, "ConstraintViolationException"} -> {:reply, {:error, :constraint_violation}}
    end
  end

  def handle_call({:delete_edge, %Edge{id: id} = edge}, _from, opts) do
    case request(:delete, edge_url(id, opts), %{}, opts) do
      :no_content -> {:noreply, opts}
      {:not_found, "RelationshipNotFoundException"} -> {:reply, {:error, :not_found}, opts}
    end
  end

  def handle_call({:vertex_properties, %Vertex{id: id} = vertex}, _from, opts) do
    case request(:get, vertex_url(id, opts) <> "/properties", %{}, opts) do
      {:ok, properties} -> {:reply, properties, opts}
      {:not_found, "NodeNotFoundException"} -> {:reply, {:error, :not_found}, opts}
    end
  end

  def handle_call({:get_vertex_property, %Vertex{id: id} = vertex, key}, _from, opts) do
    case request(:get, vertex_url(id, opts) <> "/properties/#{key}", %{}, opts) do
      {:ok, value} -> {:reply, value, opts}
      {:not_found, "NodeNotFoundException"} -> {:reply, {:error, :not_found}, opts}
      {:error, "PropertyValueException"} -> {:reply, {:error, :bad_input}}
    end
  end

  def handle_call({:set_vertex_property, %Vertex{id: id} = vertex, key, value}, _from, opts) do
    case request(:put, vertex_url(id, opts) <> "/properties/#{key}", value, opts) do
      :no_content -> {:reply, vertex, opts}
      {:not_found, "NodeNotFoundException"} -> {:reply, {:error, :not_found}, opts}
      {:error, "PropertyValueException"} -> {:reply, {:error, :bad_input}}
    end
  end

  def handle_call({:get_edge_property, %Edge{id: id} = edge, key}, _from, opts) do
    case request(:get, edge_url(id, opts) <> "/properties/#{key}", %{}, opts) do
      {:ok, value} -> {:reply, value, opts}
      {:not_found, "RelationshipNotFoundException"} -> {:reply, {:error, :not_found}, opts}
      {:error, "PropertyValueException"} -> {:reply, {:error, :bad_input}}
    end
  end

  def handle_call({:set_edge_property, %Edge{id: id} = edge, key, value}, _from, opts) do
    case request(:put, edge_url(id, opts) <> "/properties/#{key}", value, opts) do
      :no_content -> {:reply, edge, opts}
      {:not_found, "RelationshipNotFoundException"} -> {:reply, {:error, :not_found}, opts}
      {:error, "PropertyValueException"} -> {:reply, {:error, :bad_input}}
    end
  end

  def handle_call({:edge_properties, %Edge{id: id} = edge}, _from, opts) do
    case request(:get, edge_url(id, opts) <> "/properties", %{}, opts) do
      {:ok, properties} -> {:reply, properties, opts}
      {:not_found, "RelationshipNotFoundException"} -> {:reply, {:error, :not_found}, opts}
    end
  end

  def handle_call(:list_labels, _from, opts) do
    {:ok, labels} = request(:get, "/labels", %{}, opts)
    {:reply, labels, opts}
  end

  def handle_call(:list_types, _from, opts) do
    {:ok, types} = request(:get, "/relationships/types", %{}, opts)
    {:reply, types, opts}
  end

  def handle_call({:vertices_by_label, label}, _from, opts) do
    {:ok, vertices} = request(:get, "/label/#{label}/nodes", %{}, opts)
    vertices = Enum.map(vertices, &(create_vertex(&1)))
    {:reply, vertices, opts}
  end

  def handle_call({:get_labels, %Vertex{id: id} = vertex}, _from, opts) do
    case request(:get, vertex_url(id, opts) <> "/labels", %{}, opts) do
      {:ok, labels} -> {:reply, labels, opts}
      {:not_found, "NodeNotFoundException"} -> {:reply, {:error, :not_found}, opts}
    end
  end

  def handle_call({:add_labels, %Vertex{id: id, labels: labels} = vertex}, _from, opts) do
    case request(:post, vertex_url(id, opts) <> "/labels", labels, opts) do
      :no_content -> {:reply, vertex, opts}
      {:error, "BadInputException"} -> {:reply, {:error, :bad_input}, opts}
      {:not_found, "NodeNotFoundException"} -> {:reply, {:error, :not_found}, opts}
    end
  end

  def handle_call({:set_labels, %Vertex{id: id, labels: labels} = vertex}, _from, opts) do
    case request(:put, vertex_url(id, opts) <> "/labels", labels, opts) do
      :no_content -> {:reply, vertex, opts}
      {:error, "BadInputException"} -> {:reply, {:error, :bad_input}, opts}
      {:not_found, "NodeNotFoundException"} -> {:reply, {:error, :not_found}, opts}
    end
  end

  def handle_call({:remove_label, %Vertex{id: id} = vertex, label}, _from, opts) do
    case request(:delete, vertex_url(id, opts) <> "/labels" <> label, %{}, opts) do
      :no_content -> 
      vertex = update_in(vertex.labels, fn (labels) ->
        List.delete(labels, label)
      end)
      {:reply, vertex, opts}
      {:not_found, "NodeNotFoundException"} -> {:reply, {:error, :not_found}, opts}
    end
  end

  def handle_call({:all_degree, %Vertex{id: id} = vertex, types}, _from, opts) do
    case request(:get, vertex_url(id, opts) <> "/degree/all/" <> Enum.join(types, "&"), %{}, opts) do
      {:ok, degree} -> {:reply, degree, opts}
      {:not_found, "NodeNotFoundException"} -> {:reply, {:error, :not_found}, opts}
    end
  end

  def handle_call({:in_degree, %Vertex{id: id} = vertex, types}, _from, opts) do
    case request(:get, vertex_url(id, opts) <> "/degree/in/" <> Enum.join(types, "&"), %{}, opts) do
      {:ok, degree} -> {:reply, degree, opts}
      {:not_found, "NodeNotFoundException"} -> {:reply, {:error, :not_found}, opts}
    end
  end

  def handle_call({:out_degree, %Vertex{id: id} = vertex, types}, _from, opts) do
    case request(:get, vertex_url(id, opts) <> "/degree/out/" <> Enum.join(types, "&"), %{}, opts) do
      {:ok, degree} -> {:reply, degree, opts}
      {:not_found, "NodeNotFoundException"} -> {:reply, {:error, :not_found}, opts}
    end
  end

  def handle_call({:all_edges, %Vertex{id: id} = vertex, types}, _from, opts) do
    case request(:get, vertex_url(id, opts) <> "/relationships/all/" <> Enum.join(types, "&"), %{}, opts) do
      {:ok, edges} ->
        edges = Enum.map(edges, &(create_edge(&1)))
        {:reply, edges, opts}
        {:not_found, "NodeNotFoundException"} -> {:reply, {:error, :not_found}, opts}
    end
  end

  def handle_call({:in_edges, %Vertex{id: id} = vertex, types}, _from, opts) do
    case request(:get, vertex_url(id, opts) <> "/relationships/in/" <> Enum.join(types, "&"), %{}, opts) do
      {:ok, edges} ->
        edges = Enum.map(edges, &(create_edge(&1)))
        {:reply, edges, opts}
        {:not_found, "NodeNotFoundException"} -> {:reply, {:error, :not_found}, opts}
    end
  end

  def handle_call({:out_edges, %Vertex{id: id} = vertex, types}, _from, opts) do
    case request(:get, vertex_url(id, opts) <> "/relationships/out/" <> Enum.join(types, "&"), %{}, opts) do
      {:ok, edges} ->
        edges = Enum.map(edges, &(create_edge(&1)))
        {:reply, edges, opts}
        {:not_found, "NodeNotFoundException"} -> {:reply, {:error, :not_found}, opts}
    end
  end

end
