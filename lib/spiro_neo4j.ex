defmodule Spiro.Adapter.Neo4j do
  @behaviour Spiro.Adapter
  require Logger
  use GenServer

  alias Spiro.Vertex
  alias Spiro.Edge

  def start_link(opts, module) do
    GenServer.start_link(__MODULE__, opts, [name: module])
  end

  def add_vertex(vertex, module), do: GenServer.call(module, {:add_vertex, vertex})
  def add_edge(edge, module), do: GenServer.call(module, {:add_edge, edge})

  def update_vertex(vertex, module), do: GenServer.call(module, {:update_vertex, vertex})
  def update_edge(edge, module), do: GenServer.call(module, {:update_edge, edge})

  def delete_vertex(vertex, module), do: GenServer.call(module, {:delete_vertex, vertex})
  def delete_edge(edge, module), do: GenServer.call(module, {:delete_edge, edge})

  def vertex_properties(vertex, module), do: GenServer.call(module, {:vertex_properties, vertex})
  def edge_properties(edge, module), do: GenServer.call(module, {:edge_properties, edge})

  def get_vertex_property(vertex, key, module), do: GenServer.call(module, {:get_vertex_property, vertex, key})
  def set_vertex_property(vertex, key, value, module), do: GenServer.call(module, {:set_vertex_property, vertex, key, value})

  def get_edge_property(edge, key, module), do: GenServer.call(module, {:get_edge_property, edge, key})
  def set_edge_property(edge, key, value, module), do: GenServer.call(module, {:set_edge_property, edge, key, value})

  def list_labels(module), do: GenServer.call(module, :list_labels)
  def list_types(module), do: GenServer.call(module, :list_types)

  def vertices_by_label(label, module), do: GenServer.call(module, {:vertices_by_label, label})
  def get_labels(vertex, module), do: GenServer.call(module, {:get_labels, vertex})
  def add_labels(vertex, labels, module), do: GenServer.call(module, {:add_labels, vertex, labels})
  def set_labels(vertex, module), do: GenServer.call(module, {:set_labels, vertex})
  def remove_label(vertex, label, module), do: GenServer.call(module, {:remove_label, vertex, label})

  def vertex_degree(vertex, direction, types, module), do: GenServer.call(module, {:vertex_degree, vertex, direction, types})
  def adjacent_edges(vertex, direction, types, module), do: GenServer.call(module, {:adjacent_edges, vertex, direction, types})
  def vertex_neighbours(vertex, direction, types, module), do: GenServer.call(module, {:vertex_neighbours, vertex, direction, types})





  defp vertex_url(id), do: "/db/data/node/#{id}"
  defp edge_url(id), do: "/db/data/relationship/#{id}"

  defp get_id(url), do: Regex.run(~r/\d+$/, url) |> List.first |> String.to_integer

  defp mk_klist(map) do
    Enum.reduce(map, [], fn ({key, val}, acc) -> [{String.to_atom(key), val} | acc] end)
  end

  defp create_edge(edge) do
    %Edge{id: edge["metadata"]["id"],
      type: edge["type"],
      properties: mk_klist(edge["data"]),
      from: %Vertex{id: get_id(edge["start"])},
      to: %Vertex{id: get_id(edge["en" <> "d"])} }
  end
  
  defp create_vertex(vertex) do
    %Vertex{id: vertex["metadata"]["id"],
      labels: vertex["metadata"]["labels"],
      properties: mk_klist(vertex["data"]) }
  end

  defp request(method, url, params, opts) do
    url = opts[:host] <> url
    headers = [{"Accept", "application/json"},
      {"Accept", "application/json; charset=UTF-8"},
      {"Content-Type", "application/json; charset=UTF-8"},
      {"User-Agent", "Spiro"},
      {"X-Stream", "true"}]
    hackney = [basic_auth: {opts[:username], opts[:password]}]
    Logger.debug "sending #{method |> to_string |> String.upcase} request " <>
      "to #{url} with params:\n" <> String.slice(Poison.encode!(params), 0, 80)
    case HTTPoison.request!(method, url, Poison.encode!(params), headers, [hackney: hackney]) do
      %HTTPoison.Response{status_code: 204}             -> :no_content
      %HTTPoison.Response{status_code: 500}             -> {:error, :unknown_error}
      %HTTPoison.Response{status_code: 200, body: body} -> {:ok, Poison.decode!(body)}
      %HTTPoison.Response{status_code: 201, body: body} -> {:created, Poison.decode!(body)}
      %HTTPoison.Response{status_code: 400, body: body} -> {:error, Poison.decode!(body)["exception"]}
      %HTTPoison.Response{status_code: 404, body: ""}   -> {:not_found, :unknown_error}
      %HTTPoison.Response{status_code: 404, body: body} -> {:not_found, Poison.decode!(body)["exception"]}
      %HTTPoison.Response{status_code: 409, body: body} -> {:conflict, Poison.decode!(body)["exception"]}
    end
  end
  
  defp catch_errors(response, success_fun, error_fun) do
    case response do
      {:not_found, "NodeNotFoundException"} -> error_fun.(:not_found)
      {:not_found, "RelationshipNotFoundException"} -> error_fun.(:not_found)
      {:not_found, "StartNodeNotFoundException"} -> error_fun.(:not_found)
      {:not_found, :unknown_error} -> error_fun.(:not_found)
      {:conflict, "ConstraintViolationException"} -> error_fun.(:constraint_violation)
      {:error, "BadInputException"} -> error_fun.(:bad_input)
      {:error, "PropertyValueException"} -> error_fun.(:bad_input)
      {:error, :unknown_error} -> error_fun.(:unknown_error)
      {:ok, body} -> success_fun.(body)
      {:created, body} -> success_fun.(body)
      :no_content -> success_fun.(:no_content)
    end
  end

  # server callbacks
  # TODO: extract error catching in a single function
  def handle_call({:add_vertex, %Vertex{properties: properties} = vertex}, _from, opts) do
    params = properties |> Enum.into(%{})
    resp = request(:post, "/db/data/node", params, opts)
            |> catch_errors(&(%{vertex | id: &1["metadata"]["id"]}),
                            &({:error, &1}))
    {:reply, resp, opts}
  end

  def handle_call({:add_edge, %Edge{properties: properties, to: to, from: from, type: type} = edge}, _from, opts) do
    url = "/db/data/node/#{from.id}/relationships"
    params = %{to: vertex_url(to.id),
      type: type,
      data: Enum.into(properties, %{}) }
    resp = request(:post, url, params, opts)
            |> catch_errors(&(%{edge | id: &1["metadata"]["id"]}),
                            &({:error, &1}))
    {:reply, resp, opts}
  end


  def handle_call({:update_vertex, %Vertex{id: id, properties: properties} = vertex}, _from, opts) do
    params = properties |> Enum.into(%{})
    resp = request(:put, vertex_url(id) <> "/properties", params, opts)
            |> catch_errors(fn (_no_content) -> vertex end,
                            &({:error, &1}))
    {:reply, resp, opts}
  end

  def handle_call({:update_edge, %Edge{id: id, properties: properties} = edge}, _from, opts) do
    params = properties |> Enum.into(%{})
    resp = request(:put, edge_url(id) <> "/properties", params, opts)
            |> catch_errors(fn (_no_content) -> edge end,
                            &({:error, &1}))
    {:reply, resp, opts}
  end


  def handle_call({:delete_vertex, %Vertex{id: id}}, _from, opts) do
    resp = request(:delete, vertex_url(id), %{}, opts)
            |> catch_errors(fn (_no_content) -> :ok end,
                            &({:error, &1}))
    {:reply, resp, opts}
  end

  def handle_call({:delete_edge, %Edge{id: id}}, _from, opts) do
    resp = request(:delete, edge_url(id), %{}, opts)
            |> catch_errors(fn (_no_content) -> :ok end,
                            &({:error, &1}))
    {:reply, resp, opts}
  end

  def handle_call({:vertex_properties, %Vertex{id: id}}, _from, opts) do
    resp = request(:get, vertex_url(id) <> "/properties", %{}, opts)
            |> catch_errors(&(mk_klist(&1)),
                            &({:error, &1}))
    {:reply, resp, opts}
  end

  def handle_call({:edge_properties, %Edge{id: id}}, _from, opts) do
    resp = request(:get, edge_url(id) <> "/properties", %{}, opts)
            |> catch_errors(&(mk_klist(&1)),
                            &({:error, &1}))
    {:reply, resp, opts}
  end


  def handle_call({:get_vertex_property, %Vertex{id: id}, key}, _from, opts) do
    resp = request(:get, vertex_url(id) <> "/properties/#{key}", %{}, opts)
            |> catch_errors(&(&1),
                            &({:error, &1}))
    {:reply, resp, opts}
  end

  def handle_call({:set_vertex_property, %Vertex{id: id} = vertex, key, value}, _from, opts) do
    resp = request(:put, vertex_url(id) <> "/properties/#{key}", value, opts)
            |> catch_errors(fn (_no_content) -> vertex end,
                            &({:error, &1}))
    {:reply, resp, opts}
  end


  def handle_call({:get_edge_property, %Edge{id: id}, key}, _from, opts) do
    resp = request(:get, edge_url(id) <> "/properties/#{key}", %{}, opts)
            |> catch_errors(&(&1),
                            &({:error, &1}))
    {:reply, resp, opts}
  end

  def handle_call({:set_edge_property, %Edge{id: id} = edge, key, value}, _from, opts) do
    resp = request(:put, edge_url(id) <> "/properties/#{key}", value, opts)
            |> catch_errors(fn (_no_content) -> edge end,
                            &({:error, &1}))
    {:reply, resp, opts}
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
    resp = request(:get, "/label/#{label}/nodes", %{}, opts)
            |> catch_errors(fn (vertices) -> Enum.map(vertices, &(create_vertex(&1))) end,
                            &({:error, &1}))
    {:reply, resp, opts}
  end

  def handle_call({:get_labels, %Vertex{id: id}}, _from, opts) do
    resp = request(:get, vertex_url(id) <> "/labels", %{}, opts)
            |> catch_errors(&(&1),
                            &({:error, &1}))
    {:reply, resp, opts}
  end

  def handle_call({:add_labels, %Vertex{id: id} = vertex, labels}, _from, opts) do
    resp = request(:post, vertex_url(id) <> "/labels", labels, opts)
            |> catch_errors(fn (_no_content) -> vertex end,
                            &({:error, &1}))
    {:reply, resp, opts}
  end

  def handle_call({:set_labels, %Vertex{id: id, labels: labels} = vertex}, _from, opts) do
    resp = request(:put, vertex_url(id) <> "/labels", labels, opts)
            |> catch_errors(fn (_no_content) -> vertex end,
                            &({:error, &1}))
    {:reply, resp, opts}
  end

  def handle_call({:remove_label, %Vertex{id: id} = vertex, label}, _from, opts) do
    resp = request(:delete, vertex_url(id) <> "/labels/" <> label, %{}, opts)
            |> catch_errors(fn (_no_content) -> update_in(vertex.labels, &List.delete(&1, label)) end,
                            &({:error, &1}))
    {:reply, resp, opts}
  end


  def handle_call({:vertex_degree, %Vertex{id: id}, direction, types}, _from, opts) do
    url = case direction do
      :in -> vertex_url(id) <> "/degree/in/" <> Enum.join(types, "&")
      :out -> vertex_url(id) <> "/degree/out/" <> Enum.join(types, "&")
      :all -> vertex_url(id) <> "/degree/all/" <> Enum.join(types, "&")
    end
    resp = request(:get, url, %{}, opts)
            |> catch_errors(&(&1),
                            &({:error, &1}))
    {:reply, resp, opts}
  end


  def handle_call({:adjacent_edges, %Vertex{id: id}, direction, types}, _from, opts) do
    url = case direction do
      :in -> vertex_url(id) <> "/relationships/in/" <> Enum.join(types, "&")
      :out -> vertex_url(id) <> "/relationships/out/" <> Enum.join(types, "&")
      :all -> vertex_url(id) <> "/relationships/all/" <> Enum.join(types, "&")
    end
    resp = request(:get, url, %{}, opts)
            |> catch_errors(fn (edges) -> Enum.map(edges, &(create_edge(&1))) end,
                            &({:error, &1}))
    {:reply, resp, opts}
  end


  def handle_call({:vertex_neighbours, %Vertex{id: id}, direction, types}, _from, opts) do
    url = case direction do
      :in -> vertex_url(id) <> "/relationships/in/" <> Enum.join(types, "&")
      :out -> vertex_url(id) <> "/relationships/out/" <> Enum.join(types, "&")
      :all -> vertex_url(id) <> "/relationships/all/" <> Enum.join(types, "&")
    end
    resp = request(:get, url, %{}, opts)
            |> catch_errors(fn (edges) ->
              Enum.map(edges, fn (edge) ->
                edge = create_edge(edge)
                case edge.from.id do
                  ^id -> %Vertex{id: edge.to.id}
                  _ -> %Vertex{id: edge.from.id}
                end
              end)
            end,
            &({:error, &1}))
    {:reply, resp, opts}
  end

  def supported_functions, do: %{vertex_labels: true,
                                 edge_type: true,
                                 query: true}

end
