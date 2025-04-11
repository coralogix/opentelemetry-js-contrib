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
  Attributes,
  Link,
  context as otelContext,
  propagation,
  SpanKind,
  TextMapGetter,
  trace,
} from '@opentelemetry/api';
import { SemanticAttributes } from '@opentelemetry/semantic-conventions';
import { SNSEvent, SNSEventRecord } from 'aws-lambda';
import { isDefined } from '../utils';
import {
  LambdaTrigger,
  TriggerSpanInitializerResult,
  validateRecordsEvent,
} from './common';
import { TriggerOrigin } from './index';

const isSNSEvent = validateRecordsEvent<SNSEvent>(
  'aws:sns',
  ['Sns'],
  'EventSource'
);

function fetchLinks(event: SNSEvent): Link[] {
  const links = event.Records.map((record): Link | undefined => {
    const extractedContext = propagation.extract(
      otelContext.active(),
      record,
      SNSEventRecordTextMapGetter
    );
    const context = trace.getSpan(extractedContext)?.spanContext();
    if (!context) {
      return undefined;
    }
    return { context };
  }).filter(isDefined);
  return links;
}

const SNSEventRecordTextMapGetter: TextMapGetter<SNSEventRecord> = {
  get(carrier, key) {
    const attributes = carrier?.Sns?.MessageAttributes;
    const value = attributes[key]?.Value;
    return value;
  },

  keys(carrier) {
    if (!carrier) {
      return [];
    }
    return Object.keys(carrier);
  },
};

function initializeSnsSpan(event: SNSEvent): TriggerSpanInitializerResult {
  const { Records: records } = event;

  const sources = new Set(records.map(({ Sns }) => Sns.TopicArn));

  const source =
    // eslint-disable-next-line @typescript-eslint/no-non-null-assertion
    sources.size === 1 ? sources.values()!.next()!.value : 'multiple_sources';

  const attributes: Attributes = {
    [SemanticAttributes.FAAS_TRIGGER]: 'pubsub',
    'faas.trigger.type': 'SNS',
    [SemanticAttributes.MESSAGING_SYSTEM]: 'aws.sns',
    [SemanticAttributes.MESSAGING_OPERATION]: 'process',
    [SemanticAttributes.MESSAGING_URL]: source, // full topic arn
    [SemanticAttributes.MESSAGING_DESTINATION]: source?.split(':')?.at(-1), // just the function name
    'messaging.destination.name': source, // full topic arn
    'messaging.batch.message_count': records.length,
    'messaging.source.kind': 'topic',
  };
  const name = `${source} process`;
  const options = {
    kind: SpanKind.CONSUMER,
    attributes,
    links: fetchLinks(event),
  };
  return { name, options, origin: TriggerOrigin.SNS };
}

export const SNSTrigger: LambdaTrigger<SNSEvent> = {
  validator: isSNSEvent,
  initializer: initializeSnsSpan,
};
