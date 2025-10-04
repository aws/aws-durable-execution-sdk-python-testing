# Python Durable Functions Examples

## Local Testing with SAM

Test functions locally:
```bash
sam local invoke HelloWorldFunction
```

Test with custom event:
```bash
sam local invoke HelloWorldFunction -e event.json
```

## Deploy Functions

Deploy with Python script:
```bash
python3 deploy.py hello_world
```

Deploy with SAM:
```bash
sam build
sam deploy --guided
```

## Environment Variables

- `AWS_ACCOUNT_ID`: Your AWS account ID
- `LAMBDA_ENDPOINT`: Your Lambda service endpoint  
- `INVOKE_ACCOUNT_ID`: Account ID allowed to invoke functions
- `AWS_REGION`: AWS region (default: us-west-2)
- `KMS_KEY_ARN`: KMS key for encryption (optional)

## Available Examples

- **hello_world**: Simple hello world function

## Adding New Examples

1. Add your Python function to `src/`
2. Update `examples-catalog.json` and `template.yaml`
3. Deploy using either script above
