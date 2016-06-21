defmodule Graph do
  use Spiro.Graph, otp_app: :graph, adapter: Spiro.Adapter.Neo4j, adapter_opts: [host: "localhost:7474", username: "neo4j", password: "neoneo"]
end
