Embulk::JavaPlugin.register_output(
  "s3", "org.embulk.output.S3FileOutputPlugin",
  File.expand_path('../../../../classpath', __FILE__))
