/*
 * Copyright The OpenTelemetry Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

// We access through node_modules to allow it to be patched.
/* eslint-disable node/no-extraneous-require */

import * as path from 'path';

import {
  AwsLambdaInstrumentation,
  AwsLambdaInstrumentationConfig,
} from '../../src';
import {
  BatchSpanProcessor,
  InMemorySpanExporter,
  ReadableSpan,
} from '@opentelemetry/sdk-trace-base';
import { NodeTracerProvider } from '@opentelemetry/sdk-trace-node';
import { APIGatewayProxyEvent, Context } from 'aws-lambda';
import * as assert from 'assert';
import {
  SemanticAttributes,
  SemanticResourceAttributes,
} from '@opentelemetry/semantic-conventions';
import { SpanKind, SpanStatusCode } from '@opentelemetry/api';
import { assertSpanSuccess } from './lambda-handler.test';
import { APIGatewayProxyEventV2 } from 'aws-lambda/trigger/api-gateway-proxy';
import { LambdaAttributes, TriggerOrigin } from '../../src/triggers';

const memoryExporter = new InMemorySpanExporter();
const provider = new NodeTracerProvider();
provider.addSpanProcessor(new BatchSpanProcessor(memoryExporter));
provider.register();

describe('gateway API handler', () => {
  let instrumentation: AwsLambdaInstrumentation;

  let oldEnv: NodeJS.ProcessEnv;

  const ctx = {
    functionName: 'my_function',
    invokedFunctionArn: 'my_arn',
    awsRequestId: 'aws_request_id',
  } as Context;

  const initializeHandler = (
    handler: string,
    config: AwsLambdaInstrumentationConfig = {
      detectTrigger: true,
    }
  ) => {
    process.env._HANDLER = handler;

    instrumentation = new AwsLambdaInstrumentation(config);
    instrumentation.setTracerProvider(provider);
  };

  const lambdaRequire = (module: string) =>
    require(path.resolve(__dirname, '..', module));

  beforeEach(() => {
    oldEnv = { ...process.env };
    process.env.LAMBDA_TASK_ROOT = path.resolve(__dirname, '..');
  });

  afterEach(() => {
    process.env = oldEnv;
    instrumentation.disable();

    memoryExporter.reset();
  });

  describe('rest gateway span tests', () => {
    const event: APIGatewayProxyEvent = {
      resource: '/my/resource',
      path: '/my/path',
      httpMethod: 'POST',
      headers: {},
      multiValueHeaders: {},
      queryStringParameters: null,
      multiValueQueryStringParameters: null,
      pathParameters: null,
      stageVariables: null,
      body: '',
      isBase64Encoded: false,
      requestContext: {
        identity: {
          accessKey: null,
          accountId: null,
          apiKey: null,
          apiKeyId: null,
          caller: null,
          clientCert: null,
          cognitoAuthenticationProvider: null,
          cognitoAuthenticationType: null,
          cognitoIdentityId: null,
          cognitoIdentityPoolId: null,
          principalOrgId: null,
          sourceIp: '',
          user: null,
          userAgent: null,
          userArn: null,
        },
        accountId: '123456789012',
        apiId: 'id',
        authorizer: {
          claims: null,
          scopes: null,
          principalId: null,
        },
        domainName: 'id.execute-api.us-east-1.amazonaws.com',
        domainPrefix: 'id',
        extendedRequestId: 'request-id',
        httpMethod: 'GET',
        path: '/my/path',
        protocol: 'HTTP/1.1',
        requestId: 'id=',
        requestTime: '04/Mar/2020:19:15:17 +0000',
        requestTimeEpoch: 1583349317135,
        resourceId: 'my-resource',
        resourcePath: '/my/path',
        stage: '$default',
      },
    };

    const assertRestGatewaySpanSuccess = (span: ReadableSpan) => {
      assert.strictEqual(span.kind, SpanKind.SERVER);
      assert.strictEqual(span.name, event.resource);

      assert.strictEqual(span.status.code, SpanStatusCode.OK);
      assert.strictEqual(span.status.message, undefined);

      assert.strictEqual(
        span.attributes[LambdaAttributes.TRIGGER_SERVICE],
        TriggerOrigin.API_GATEWAY_REST
      );

      assert.strictEqual(
        span.attributes[SemanticAttributes.HTTP_METHOD],
        event.requestContext.httpMethod
      );
      assert.strictEqual(
        span.attributes[SemanticAttributes.HTTP_ROUTE],
        event.requestContext.resourcePath
      );

      assert.strictEqual(
        span.attributes[SemanticAttributes.HTTP_SERVER_NAME],
        event.requestContext.domainName
      );
      assert.strictEqual(
        span.attributes[SemanticResourceAttributes.CLOUD_ACCOUNT_ID],
        event.requestContext.accountId
      );
    };

    const assertRestGatewaySpanFailure = (span: ReadableSpan) => {
      assert.strictEqual(
        span.attributes[LambdaAttributes.TRIGGER_SERVICE],
        TriggerOrigin.API_GATEWAY_REST
      );
      assert.strictEqual(span.kind, SpanKind.SERVER);
      assert.strictEqual(span.name, event.resource);

      assert.strictEqual(span.status.code, SpanStatusCode.ERROR);
    };

    it('should export two valid span', async () => {
      initializeHandler('lambda-test/gateway.handler');

      const result = await lambdaRequire('lambda-test/gateway').handler(
        event,
        ctx
      );

      assert.strictEqual(result.statusCode, 200);
      const spans = memoryExporter.getFinishedSpans();
      assert.strictEqual(spans.length, 2);
      const [spanLambda, spanGateway] = spans;
      assertSpanSuccess(spanLambda);
      assertRestGatewaySpanSuccess(spanGateway);
      assert.strictEqual(
        spanGateway.attributes[SemanticAttributes.HTTP_STATUS_CODE],
        200
      );
      assert.strictEqual(spanGateway.parentSpanId, undefined);
      assert.strictEqual(
        spanLambda.parentSpanId,
        spanGateway.spanContext().spanId
      );
    });

    it('gateway span should reject when returning 500 ', async () => {
      initializeHandler('lambda-test/gateway.error500');

      const result = await lambdaRequire('lambda-test/gateway').error500(
        event,
        ctx
      );

      assert.strictEqual(result.statusCode, 500);

      const spans = memoryExporter.getFinishedSpans();
      assert.strictEqual(spans.length, 2);
      const [spanLambda, spanGateway] = spans;

      assert.strictEqual(
        spanGateway.status.message,
        'Return to API Gateway with error 500'
      );

      assertSpanSuccess(spanLambda);
      assertRestGatewaySpanFailure(spanGateway);
      assert.strictEqual(spanGateway.parentSpanId, undefined);
      assert.strictEqual(
        spanGateway.attributes[SemanticAttributes.HTTP_STATUS_CODE],
        500
      );

      assert.strictEqual(
        spanLambda.parentSpanId,
        spanGateway.spanContext().spanId
      );
    });

    it('gateway span should reject when returning 400 ', async () => {
      initializeHandler('lambda-test/gateway.error400');

      const result = await lambdaRequire('lambda-test/gateway').error400(
        event,
        ctx
      );

      assert.strictEqual(result.statusCode, 400);
      const spans = memoryExporter.getFinishedSpans();
      assert.strictEqual(spans.length, 2);

      const [_, spanGateway] = spans;

      assert.strictEqual(
        spanGateway.attributes[SemanticAttributes.HTTP_STATUS_CODE],
        400
      );
      assert.strictEqual(
        spanGateway.status.message,
        'Return to API Gateway with error 400'
      );
      assertRestGatewaySpanFailure(spanGateway);
    });

    it('gateway span should reject when returning 400 sync', async () => {
      initializeHandler('lambda-test/gateway.error400Sync');

      await new Promise((resolve, reject) => {
        lambdaRequire('lambda-test/gateway').error400Sync(
          event,
          ctx,
          (err: Error, res: any) => {
            if (err) {
              reject(err);
            } else {
              resolve(res);
            }
          }
        );
      });

      const spans = memoryExporter.getFinishedSpans();
      assert.strictEqual(spans.length, 2);
      const [_, spanGateway] = spans;

      assert.strictEqual(
        spanGateway.attributes[SemanticAttributes.HTTP_STATUS_CODE],
        400
      );
      assert.strictEqual(
        spanGateway.status.message,
        'Return to API Gateway with error 400'
      );
      assertRestGatewaySpanFailure(spanGateway);
    });

    it('gateway span should reject when throwing error', async () => {
      initializeHandler('lambda-test/gateway.errorAsync');

      //let err: any;
      try {
        await lambdaRequire('lambda-test/gateway').errorAsync(event, ctx);
      } catch (e) {
        //err = e;
      }

      const spans = memoryExporter.getFinishedSpans();
      assert.strictEqual(spans.length, 2);
      const [_, spanGateway] = spans;
      assert.strictEqual(spanGateway.status.message, 'handler error');
    });

    it('gateway span contains string response body when returned from lambda', async () => {
      initializeHandler('lambda-test/gateway.stringBody');

      await lambdaRequire('lambda-test/gateway').stringBody(event, ctx);

      const spans = memoryExporter.getFinishedSpans();
      assert.strictEqual(spans.length, 2);
      const [_, spanGateway] = spans;
      assert.strictEqual(
        spanGateway.attributes['http.response.body'],
        'string-response'
      );
    });

    it('gateway span contain stringified json response body when returned from lambda', async () => {
      initializeHandler('lambda-test/gateway.jsonBody');

      await lambdaRequire('lambda-test/gateway').jsonBody(event, ctx);

      const spans = memoryExporter.getFinishedSpans();
      assert.strictEqual(spans.length, 2);
      const [_, spanGateway] = spans;
      assert.strictEqual(
        spanGateway.attributes['http.response.body'],
        JSON.stringify({ val: 'json-response' })
      );
    });

    it('gateway span should not be added if body does not include resource', async () => {
      initializeHandler('lambda-test/gateway.handler');

      const { resource, ...invalidEvent } = event;
      const result = await lambdaRequire('lambda-test/gateway').handler(
        invalidEvent,
        ctx
      );

      assert.strictEqual(result.statusCode, 200);
      const spans = memoryExporter.getFinishedSpans();
      assert.strictEqual(spans.length, 1);
    });

    it('gateway span should not be added if body does not include requestContext', async () => {
      initializeHandler('lambda-test/gateway.handler');

      const { requestContext, ...invalidEvent } = event;
      const result = await lambdaRequire('lambda-test/gateway').handler(
        invalidEvent,
        ctx
      );

      assert.strictEqual(result.statusCode, 200);
      const spans = memoryExporter.getFinishedSpans();
      assert.strictEqual(spans.length, 1);
    });

    describe('http gateway span tests', () => {
      const event: APIGatewayProxyEventV2 = {
        version: '2.0',
        routeKey: 'ANY /',
        rawPath: '/default/',
        rawQueryString: '',
        headers: {},
        requestContext: {
          accountId: '123456789012',
          apiId: 'id',
          domainName: 'id.execute-api.us-east-1.amazonaws.com',
          domainPrefix: 'id',
          http: {
            method: 'GET',
            path: '/default/',
            protocol: 'HTTP/1.1',
            sourceIp: '0.0.0.0',
            userAgent: 'agent',
          },
          requestId: 'en68JjNcjoEEPgA=',
          routeKey: 'ANY /',
          stage: 'default',
          time: '12/Jan/2023:09:52:13 +0000',
          timeEpoch: 1673517133563,
        },
        isBase64Encoded: false,
      };

      const assertHttpGatewaySpanSuccess = (span: ReadableSpan) => {
        assert.strictEqual(span.kind, SpanKind.SERVER);
        assert.strictEqual(span.name, event.rawPath);

        assert.strictEqual(span.status.code, SpanStatusCode.OK);
        assert.strictEqual(span.status.message, undefined);
        assert.strictEqual(
          span.attributes[LambdaAttributes.TRIGGER_SERVICE],
          TriggerOrigin.API_GATEWAY_HTTP
        );
        assert.strictEqual(
          span.attributes[SemanticAttributes.HTTP_METHOD],
          event.requestContext.http.method
        );
        assert.strictEqual(
          span.attributes[SemanticAttributes.HTTP_TARGET],
          event.rawPath
        );

        assert.strictEqual(
          span.attributes[SemanticAttributes.HTTP_SERVER_NAME],
          event.requestContext.domainName
        );
        assert.strictEqual(
          span.attributes[SemanticResourceAttributes.CLOUD_ACCOUNT_ID],
          event.requestContext.accountId
        );
      };

      const assertHttpGatewaySpanFailure = (span: ReadableSpan) => {
        assert.strictEqual(span.kind, SpanKind.SERVER);
        assert.strictEqual(span.name, event.rawPath);

        assert.strictEqual(
          span.attributes[LambdaAttributes.TRIGGER_SERVICE],
          TriggerOrigin.API_GATEWAY_HTTP
        );
        assert.strictEqual(span.status.code, SpanStatusCode.ERROR);
      };

      it('should export two valid span', async () => {
        initializeHandler('lambda-test/gateway.handler');

        const result = await lambdaRequire('lambda-test/gateway').handler(
          event,
          ctx
        );

        assert.strictEqual(result.statusCode, 200);
        const spans = memoryExporter.getFinishedSpans();
        assert.strictEqual(spans.length, 2);
        const [spanLambda, spanGateway] = spans;
        assertSpanSuccess(spanLambda);
        assertHttpGatewaySpanSuccess(spanGateway);
        assert.strictEqual(
          spanGateway.attributes[SemanticAttributes.HTTP_STATUS_CODE],
          200
        );
        assert.strictEqual(spanGateway.parentSpanId, undefined);
        assert.strictEqual(
          spanLambda.parentSpanId,
          spanGateway.spanContext().spanId
        );
      });

      it('gateway span should reject when returning 500 ', async () => {
        initializeHandler('lambda-test/gateway.error500');

        const result = await lambdaRequire('lambda-test/gateway').error500(
          event,
          ctx
        );

        assert.strictEqual(result.statusCode, 500);

        const spans = memoryExporter.getFinishedSpans();
        assert.strictEqual(spans.length, 2);
        const [spanLambda, spanGateway] = spans;

        assert.strictEqual(
          spanGateway.status.message,
          'Return to API Gateway with error 500'
        );

        assertSpanSuccess(spanLambda);
        assertHttpGatewaySpanFailure(spanGateway);
        assert.strictEqual(spanGateway.parentSpanId, undefined);
        assert.strictEqual(
          spanGateway.attributes[SemanticAttributes.HTTP_STATUS_CODE],
          500
        );

        assert.strictEqual(
          spanLambda.parentSpanId,
          spanGateway.spanContext().spanId
        );
      });

      it('gateway span should reject when returning 400 ', async () => {
        initializeHandler('lambda-test/gateway.error400');

        const result = await lambdaRequire('lambda-test/gateway').error400(
          event,
          ctx
        );

        assert.strictEqual(result.statusCode, 400);
        const spans = memoryExporter.getFinishedSpans();
        assert.strictEqual(spans.length, 2);

        const [_, spanGateway] = spans;

        assert.strictEqual(
          spanGateway.attributes[SemanticAttributes.HTTP_STATUS_CODE],
          400
        );
        assert.strictEqual(
          spanGateway.status.message,
          'Return to API Gateway with error 400'
        );
        assertHttpGatewaySpanFailure(spanGateway);
      });

      it('gateway span should reject when returning 400 sync', async () => {
        initializeHandler('lambda-test/gateway.error400Sync');

        await new Promise((resolve, reject) => {
          lambdaRequire('lambda-test/gateway').error400Sync(
            event,
            ctx,
            (err: Error, res: any) => {
              if (err) {
                reject(err);
              } else {
                resolve(res);
              }
            }
          );
        });

        const spans = memoryExporter.getFinishedSpans();
        assert.strictEqual(spans.length, 2);
        const [_, spanGateway] = spans;

        assert.strictEqual(
          spanGateway.attributes[SemanticAttributes.HTTP_STATUS_CODE],
          400
        );
        assert.strictEqual(
          spanGateway.status.message,
          'Return to API Gateway with error 400'
        );
        assertHttpGatewaySpanFailure(spanGateway);
      });

      it('gateway span should reject when throwing error', async () => {
        initializeHandler('lambda-test/gateway.errorAsync');

        //let err: any;
        try {
          await lambdaRequire('lambda-test/gateway').errorAsync(event, ctx);
        } catch (e) {
          //err = e;
        }

        const spans = memoryExporter.getFinishedSpans();
        assert.strictEqual(spans.length, 2);
        const [_, spanGateway] = spans;
        assert.strictEqual(spanGateway.status.message, 'handler error');
      });

      it('gateway span contains string response body when returned from lambda', async () => {
        initializeHandler('lambda-test/gateway.stringBody');

        await lambdaRequire('lambda-test/gateway').stringBody(event, ctx);

        const spans = memoryExporter.getFinishedSpans();
        assert.strictEqual(spans.length, 2);
        const [_, spanGateway] = spans;
        assert.strictEqual(
          spanGateway.attributes['http.response.body'],
          'string-response'
        );
      });

      it('gateway span contain stringified json response body when returned from lambda', async () => {
        initializeHandler('lambda-test/gateway.jsonBody');

        await lambdaRequire('lambda-test/gateway').jsonBody(event, ctx);

        const spans = memoryExporter.getFinishedSpans();
        assert.strictEqual(spans.length, 2);
        const [_, spanGateway] = spans;
        assert.strictEqual(
          spanGateway.attributes['http.response.body'],
          JSON.stringify({ val: 'json-response' })
        );
      });

      it('gateway span should not be added if body does not include resource', async () => {
        initializeHandler('lambda-test/gateway.handler');

        const { rawPath, ...invalidEvent } = event;
        const result = await lambdaRequire('lambda-test/gateway').handler(
          invalidEvent,
          ctx
        );

        assert.strictEqual(result.statusCode, 200);
        const spans = memoryExporter.getFinishedSpans();
        assert.strictEqual(spans.length, 1);
      });

      it('gateway span should not be added if body does not include requestContext', async () => {
        initializeHandler('lambda-test/gateway.handler');

        const { requestContext, ...invalidEvent } = event;
        const result = await lambdaRequire('lambda-test/gateway').handler(
          invalidEvent,
          ctx
        );

        assert.strictEqual(result.statusCode, 200);
        const spans = memoryExporter.getFinishedSpans();
        assert.strictEqual(spans.length, 1);
      });
    });
  });
});
