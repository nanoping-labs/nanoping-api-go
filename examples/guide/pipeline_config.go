package guide

// PipelineConfig is a pipeline that sends traffic from a traffic source to a
// traffic sink on the same node.
const PipelineConfig = `{
  "version": 13,
  "plumr_config": {
    "pipeline": {
      "traffic_sink-1": {
        "traffic_sink": {
          "input": "[traffic_sink-1|in:0]-[uniform_traffic_source-1|out:0]",
          "mtu": 1500
        }
      },
      "uniform_traffic_source-1": {
        "uniform_traffic_source": {
          "output": "[traffic_sink-1|in:0]-[uniform_traffic_source-1|out:0]",
          "total_packets": 5000,
          "interval": 25
        }
      }
    }
  }
}`
