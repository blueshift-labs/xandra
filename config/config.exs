import Config

config :logger, level: :debug

config :logger, :console,
  format: "[$level] $message, $metadata\n",
  metadata: [:module]
