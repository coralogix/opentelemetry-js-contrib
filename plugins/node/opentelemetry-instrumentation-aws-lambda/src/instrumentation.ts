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

import {
  InstrumentationBase,
  safeExecuteInTheMiddle,
} from '@opentelemetry/instrumentation';
import {
  Context as OtelContext,
  context as otelContext,
  diag,
  propagation,
  ROOT_CONTEXT,
  MeterProvider,
  Span,
  SpanKind,
  SpanStatusCode,
  TextMapGetter,
  trace,
  TraceFlags,
  TracerProvider,
} from '@opentelemetry/api';
import {
  AWSXRAY_TRACE_ID_HEADER,
  AWSXRayPropagator,
} from '@opentelemetry/propagator-aws-xray';
import {
  SemanticAttributes,
  SemanticResourceAttributes,
} from '@opentelemetry/semantic-conventions';

import {
  APIGatewayProxyEventHeaders,
  Callback,
  Context,
  Handler,
} from 'aws-lambda';

import { AwsLambdaInstrumentationConfig, EventContextExtractor } from './types';
import { VERSION } from './version';
import { env } from 'process';
import { strict } from 'assert';
import {
  finalizeSpan,
  getEventTrigger,
  LambdaAttributes,
  TriggerOrigin,
} from './triggers';

diag.debug("Loading AwsLambdaInstrumentation")

const awsPropagator = new AWSXRayPropagator();
const headerGetter: TextMapGetter<APIGatewayProxyEventHeaders> = {
  keys(carrier): string[] {
    return Object.keys(carrier);
  },
  get(carrier, key: string) {
    return carrier[key];
  },
};

export const traceContextEnvironmentKey = '_X_AMZN_TRACE_ID';
export const xForwardProto = 'X-Forwarded-Proto';

export class AwsLambdaInstrumentation extends InstrumentationBase {
  private triggerOrigin: TriggerOrigin | undefined;
  private _traceForceFlusher?: () => Promise<void>;
  private _metricForceFlusher?: () => Promise<void>;

  constructor(protected override _config: AwsLambdaInstrumentationConfig = {}) {
    super('@opentelemetry/instrumentation-aws-lambda', VERSION, _config);
    if (this._config.disableAwsContextPropagation == null) {
      if (
        typeof env['OTEL_LAMBDA_DISABLE_AWS_CONTEXT_PROPAGATION'] ===
          'string' &&
        env[
          'OTEL_LAMBDA_DISABLE_AWS_CONTEXT_PROPAGATION'
        ].toLocaleLowerCase() === 'true'
      ) {
        this._config.disableAwsContextPropagation = true;
      }
    }
  }

  override setConfig(config: AwsLambdaInstrumentationConfig = {}) {
    this._config = config;
  }

  init() {
    return [];
  }

  public getPatchHandler(original: Handler): Handler {
    diag.debug('patch handler function');
    const plugin = this;

    return function patchedHandler(
      this: never,
      // The event can be a user type, it truly is any.
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      event: any,
      context: Context,
      callback: Callback
    ) {
      diag.debug(`patchedHandler 1`);
      const config = plugin._config;
      const parent = AwsLambdaInstrumentation._determineParent(
        event,
        context,
        config.disableAwsContextPropagation === true,
        config.eventContextExtractor ||
          AwsLambdaInstrumentation._defaultEventContextExtractor
      );

      const name = context.functionName;

      diag.debug(`patchedHandler 2`);
      const { triggerSpan, triggerOrigin } =
        AwsLambdaInstrumentation._getTriggerSpan(plugin, event, parent) ?? {};
      plugin.triggerOrigin = triggerOrigin;

      const inner = (otelContextInstance: OtelContext) => {
        diag.debug(`inner 1`);
        const lambdaSpan = plugin.tracer.startSpan(
          name,
          {
            kind: SpanKind.SERVER,
            attributes: {
              [SemanticAttributes.FAAS_EXECUTION]: context.awsRequestId,
              [SemanticResourceAttributes.FAAS_ID]: context.invokedFunctionArn,
              [SemanticResourceAttributes.CLOUD_ACCOUNT_ID]:
                AwsLambdaInstrumentation._extractAccountId(
                  context.invokedFunctionArn
                ),
            },
          },
          otelContextInstance
        );
        diag.debug(`inner 2`);

        if (config.requestHook) {
          safeExecuteInTheMiddle(
            () => {
              diag.debug(`inner 3`);
              return config.requestHook!(lambdaSpan, { event, context })
            },
            e => {
              diag.debug(`inner 4`);
              if (e)
                diag.error('aws-lambda instrumentation: requestHook error', e);
            },
            true
          );
        }

        diag.debug(`inner 5`);
        return otelContext.with(
          trace.setSpan(otelContextInstance, lambdaSpan),
          () => {
            diag.debug(`inner 6`);
            // Lambda seems to pass a callback even if handler is of Promise form, so we wrap all the time before calling
            // the handler and see if the result is a Promise or not. In such a case, the callback is usually ignored. If
            // the handler happened to both call the callback and complete a returned Promise, whichever happens first will
            // win and the latter will be ignored.
            const wrappedCallback = plugin._wrapCallback(
              config,
              callback,
              lambdaSpan,
              triggerSpan
            );

            diag.debug(`inner 7`);
            const maybePromise = safeExecuteInTheMiddle(
              () => {
                diag.debug(`inner 8`);
                return original.apply(this, [event, context, wrappedCallback])
              },
              error => {
                diag.debug(`inner 9`);
                if (error != null) {
                  diag.debug(`inner 9.1`);
                  // Exception thrown synchronously before resolving callback / promise.
                  // Callback may or may not have been called, we can't know for sure, but it doesn't matter, both will end the current span
                  plugin._applyResponseHook(lambdaSpan, error);
                  plugin._endSpan(lambdaSpan, error);
                }
              }
            ) as Promise<{}> | undefined;
            if (typeof maybePromise?.then === 'function') {
              diag.debug(`inner 10`);
              return maybePromise.then(
                value => {
                  diag.debug(`inner 11`);
                  plugin._applyResponseHook(lambdaSpan, null, value);
                  plugin._endSpan(lambdaSpan, undefined);
                  diag.debug(`inner 12`);
                  return value;
                },
                (err: Error | string) => {
                  diag.debug(`inner 13`);
                  plugin._applyResponseHook(lambdaSpan, err);

                  plugin._endSpan(lambdaSpan, err);
                  diag.debug(`inner 14`);
                  throw err;
                }
              );
            }
            diag.debug(`inner 15`);
            return maybePromise;
          }
        );
      };

      diag.debug(`patchedHandler 3`);
      let handlerReturn: Promise<any> | undefined;
      if (!triggerSpan) {
        // No wrapper span
        try {
          diag.debug(`patchedHandler 4`);
          handlerReturn = inner(parent);
          diag.debug(`patchedHandler 5`);
        } catch (e) {
          diag.debug(`patchedHandler 6.1`);
          // Catching a lambda that synchronously failed

          void plugin._flush();
          diag.debug(`patchedHandler 6.2`);
          throw e;
        }
      } else {
        diag.debug(`patchedHandler 7`);
        const subCtx = trace.setSpan(parent, triggerSpan);
        diag.debug(`patchedHandler 8`);
        handlerReturn = otelContext.with(subCtx, () => {
          return safeExecuteInTheMiddle(
            () => {
              diag.debug(`patchedHandler 9`);
              const innerResult = inner(subCtx); // This call never fails, because it either returns a promise, or was called with safeExecuteInTheMiddle
              // The handler was an async, it returned a promise.
              diag.debug(`patchedHandler 9`);
              if (typeof innerResult?.then === 'function') {
                diag.debug(`patchedHandler 10`);
                return innerResult.then(
                  value => {
                    diag.debug(`patchedHandler 11`);
                    strict(triggerSpan);

                    void plugin._endWrapperSpan(config, triggerSpan, value, undefined);
                    diag.debug(`patchedHandler 12`);
                    return value;
                  },
                  async error => {
                    diag.debug(`patchedHandler 13`);
                    strict(triggerSpan);
                    await plugin._endWrapperSpan(config, triggerSpan, undefined, error);
                    diag.debug(`patchedHandler 14`);
                    throw error; // We don't want the instrumentation to hide the error from AWS
                  }
                );
              } else {
                diag.debug(`patchedHandler 15`);
                // The lambda was synchronous, or it as synchronously thrown an error
                strict(triggerSpan);
                diag.debug(`patchedHandler 16`);

                //if (hasLambdaSynchronouslyThrown) {
                void plugin._endWrapperSpan(
                  config,
                  triggerSpan,
                  innerResult,
                  undefined
                );
                diag.debug(`patchedHandler 17`);
                // }
                // Fallthrough: sync reply, but callback may be in use. No way to query the event loop !
              }
              diag.debug(`patchedHandler 18`);
              return innerResult;
            },
            error => {
              diag.debug(`patchedHandler 19`);
              if (error) {
                diag.debug(`patchedHandler 20`);
                strict(triggerSpan);
                void plugin._endWrapperSpan(config, triggerSpan, undefined, error);
                diag.debug(`patchedHandler 21`);
                void plugin._flush();
              }
            }
          );
        });
      }

      diag.debug(`patchedHandler 22`);
      // Second case, lambda was asynchronous, in which case
      if (typeof handlerReturn?.then === 'function') {
        diag.debug(`patchedHandler 23`);
        return handlerReturn.then(
          async success => {
            diag.debug(`patchedHandler 24.0`);
            await plugin._flush();
            diag.debug(`patchedHandler 24.1`);
            return success;
          },
          async error => {
            diag.debug(`patchedHandler 25.0`);
            await plugin._flush();
            diag.debug(`patchedHandler 25.1`);
            throw error;
          }
        );
      }

      diag.debug(`patchedHandler 26`);
      // Third case, the lambda is purely synchronous, without event loop, nor callback() being called
      // Pitfall, no flushing !
      // We can't know for sure if the event loop is empty or not, so we can't know if we should flush or not.
      return handlerReturn;
    };
  }

  private static _getTriggerSpan(
    plugin: AwsLambdaInstrumentation,
    event: unknown,
    parentContext: OtelContext
  ): { triggerOrigin: TriggerOrigin; triggerSpan: Span } | undefined {
    if (plugin._config.detectTrigger === false) {
      return undefined;
    }
    const trigger = getEventTrigger(event);
    if (!trigger) {
      return undefined;
    }
    const { name, options, origin } = trigger;
    if (!options.attributes) {
      options.attributes = {};
    }
    options.attributes[LambdaAttributes.TRIGGER_SERVICE] = origin;
    const triggerSpan = plugin.tracer.startSpan(name, options, parentContext);
    return { triggerOrigin: origin, triggerSpan };
  }

  private async _endWrapperSpan(
    config: AwsLambdaInstrumentationConfig,
    span: Span,
    lambdaResponse: any,
    errorFromLambda: string | Error | null | undefined,
  ) {
    if (span.isRecording()) {
      if (errorFromLambda) {
        span.recordException(errorFromLambda);

        span.setStatus({
          code: SpanStatusCode.ERROR,
          message: this._errorToString(errorFromLambda),
        });

        span.end();
        return;
      }

      if (
        this.triggerOrigin !== undefined &&
        [TriggerOrigin.API_GATEWAY_REST, TriggerOrigin.API_GATEWAY_HTTP].includes(
          this.triggerOrigin
        )
      ) {
        finalizeSpan(config, this.triggerOrigin, span, lambdaResponse);
      }
      span.end();
    } else {
      diag.debug('Ending wrapper span for the second time');
    }
  }

  private _wrapCallback(
    config: AwsLambdaInstrumentationConfig,
    originalAWSLambdaCallback: Callback,
    span: Span,
    wrapperSpan?: Span
  ): Callback {
    diag.debug(`_wrapCallback 1`);
    const plugin = this;
    return (err, res) => {
      diag.debug(`_wrapCallback 2`);
      diag.debug('executing wrapped lookup callback function');
      plugin._applyResponseHook(span, err, res);

      plugin._endSpan(span, err);
      if (wrapperSpan) {
        void plugin._endWrapperSpan(config, wrapperSpan, res, err);
      }

      void this._flush().then(() => {
        diag.debug(`_wrapCallback 3`);
        diag.debug('executing original lookup callback function');
        originalAWSLambdaCallback.apply(this, [err, res]); // End of the function
      });
    };
  }

  private async _flush() {
    diag.debug('_flush 1');
    const flushers = [];
    if (this._traceForceFlusher) {
      flushers.push(this._traceForceFlusher());
    } else {
      diag.error(
        'Spans may not be exported for the lambda function because we are not force flushing before callback.'
      );
    }
    if (this._metricForceFlusher) {
      flushers.push(this._metricForceFlusher());
    } else {
      diag.error(
        'Metrics may not be exported for the lambda function because we are not force flushing before callback.'
      );
    }

    try {
      await Promise.all(flushers);
      diag.debug('_flush 2');
    } catch (e) {
      // We must not fail this call, but we may log it
      diag.error('Error while flushing the lambda', e);
    }
  }

  private _endSpan(span: Span, err: string | Error | null | undefined) {
    if (span.isRecording()) {
      if (err) {
        span.recordException(err);
      }

      const errMessage = this._errorToString(err);
      if (errMessage) {
        span.setStatus({
          code: SpanStatusCode.ERROR,
          message: errMessage,
        });
      }
      span.end();
    } else {
      diag.debug('Ending span for the second time');
    }
  }

  private _errorToString(err: string | Error | null | undefined) {
    let errMessage;
    if (typeof err === 'string') {
      errMessage = err;
    } else if (err) {
      errMessage = err.message;
    }
    return errMessage;
  }

  override setTracerProvider(tracerProvider: TracerProvider) {
    super.setTracerProvider(tracerProvider);
    this._traceForceFlusher = this._traceForceFlush(tracerProvider);
  }

  private _traceForceFlush(tracerProvider: TracerProvider) {
    if (!tracerProvider) return undefined;

    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    let currentProvider: any = tracerProvider;

    if (typeof currentProvider.getDelegate === 'function') {
      currentProvider = currentProvider.getDelegate();
    }

    if (typeof currentProvider.forceFlush === 'function') {
      return currentProvider.forceFlush.bind(currentProvider);
    }

    return undefined;
  }

  override setMeterProvider(meterProvider: MeterProvider) {
    super.setMeterProvider(meterProvider);
    this._metricForceFlusher = this._metricForceFlush(meterProvider);
  }

  private _metricForceFlush(meterProvider: MeterProvider) {
    if (!meterProvider) return undefined;

    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    const currentProvider: any = meterProvider;

    if (typeof currentProvider.forceFlush === 'function') {
      return currentProvider.forceFlush.bind(currentProvider);
    }

    return undefined;
  }

  private _applyResponseHook(
    span: Span,
    err?: Error | string | null,
    res?: any
  ) {
    if (this._config?.responseHook) {
      safeExecuteInTheMiddle(
        () => this._config.responseHook!(span, { err, res }),
        e => {
          if (e)
            diag.error('aws-lambda instrumentation: responseHook error', e);
        },
        true
      );
    }
  }

  private static _extractAccountId(arn: string): string | undefined {
    const parts = arn.split(':');
    if (parts.length >= 5) {
      return parts[4];
    }
    return undefined;
  }

  private static _defaultEventContextExtractor(event: any): OtelContext {
    // The default extractor tries to get sampled trace header from HTTP headers.
    const httpHeaders = event.headers || {};
    return propagation.extract(otelContext.active(), httpHeaders, headerGetter);
  }

  private static _determineParent(
    event: any,
    context: Context,
    disableAwsContextPropagation: boolean,
    eventContextExtractor: EventContextExtractor
  ): OtelContext {
    let parent: OtelContext | undefined = undefined;
    if (!disableAwsContextPropagation) {
      const lambdaTraceHeader = process.env[traceContextEnvironmentKey];
      if (lambdaTraceHeader) {
        parent = awsPropagator.extract(
          otelContext.active(),
          { [AWSXRAY_TRACE_ID_HEADER]: lambdaTraceHeader },
          headerGetter
        );
      }
      if (parent) {
        const spanContext = trace.getSpan(parent)?.spanContext();
        if (
          spanContext &&
          (spanContext.traceFlags & TraceFlags.SAMPLED) === TraceFlags.SAMPLED
        ) {
          // Trace header provided by Lambda only sampled if a sampled context was propagated from
          // an upstream cloud service such as S3, or the user is using X-Ray. In these cases, we
          // need to use it as the parent.
          return parent;
        }
      }
    }
    const extractedContext = safeExecuteInTheMiddle(
      () => eventContextExtractor(event, context),
      e => {
        if (e)
          diag.error(
            'aws-lambda instrumentation: eventContextExtractor error',
            e
          );
      },
      true
    );
    if (extractedContext && trace.getSpan(extractedContext)?.spanContext()) {
      return extractedContext;
    }
    if (!parent) {
      // No context in Lambda environment or HTTP headers.
      return ROOT_CONTEXT;
    }
    return parent;
  }
}
