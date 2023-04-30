# netunicorn-connector-aws
This is an AWS Fargate connector for netunicorn.

## How to use
This connector is supposed to be installed as a part of netunicorn-director-infrastructure package or container.

Install the package:
```bash
pip install netunicorn-connector-aws
```

Then, add the connector to the netunicorn-director-infrastructure configuration:
```yaml
  aws-fargate:  # unique name
    enabled: true
    module: "netunicorn.director.infrastructure.connectors.aws"  # where to import from
    class: "AWSFargate"  # class name
    config: "configuration-example.yaml"     # path to configuration file
```

Modify the configuration file to provide needed parameters (see [example](configuration-example.yaml)), such as
AWS credentials, region, etc.