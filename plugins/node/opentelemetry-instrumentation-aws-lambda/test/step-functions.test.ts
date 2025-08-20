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

import { StepFunctionsTrigger } from '../src/triggers/step-functions';
import { TriggerOrigin } from '../src/triggers/index';
import { SpanKind } from '@opentelemetry/api';
import * as assert from 'assert';

const mockStepFunctionsEvent = {
  input: {},
  executionContext: {
    Id: 'arn:aws:states:eu-west-1:396913735602:express:StateMachine-tYitg6Hm7nTT:7656396a-d4d9-4fbd-9ce5-18aab527709d:4478e987-7786-4b83-ba89-8b1ae7e9d24f',
    Input: {},
    Name: '7656396a-d4d9-4fbd-9ce5-18aab527709d',
    RoleArn:
      'arn:aws:iam::396913735602:role/nodejs-otel-israel3-StateMachineRole-I5LD8OUdmJiS',
    StartTime: '2025-08-19T12:12:08.034Z',
    RedriveCount: 0,
  },
  stateContext: {
    Name: 'Invoke Lambda function',
    EnteredTime: '2025-08-19T14:54:45.858Z',
    RetryCount: 0,
  },
  stateMachineContext: {
    Id: 'arn:aws:states:eu-west-1:396913735602:stateMachine:StateMachine-tYitg6Hm7nTT',
    Name: 'StateMachine-tYitg6Hm7nTT',
  },
};

const invalidEvent = {
  not: 'a step functions event',
};

describe('Step Functions Trigger', () => {
  it('should validate Step Functions event', () => {
    assert.strictEqual(
      StepFunctionsTrigger.validator(mockStepFunctionsEvent),
      true
    );
    assert.strictEqual(StepFunctionsTrigger.validator(invalidEvent), false);

    assert.ok(!StepFunctionsTrigger.validator(null));
    assert.ok(!StepFunctionsTrigger.validator(undefined));
  });

  it('should validate Step Functions event with invalid stateContext', () => {
    const eventWithInvalidStateContext = {
      ...mockStepFunctionsEvent,
      stateContext: {
        Name: 123, // Invalid type
        EnteredTime: '2025-08-19T14:54:45.858Z',
        RetryCount: 0,
      },
    };

    assert.strictEqual(
      StepFunctionsTrigger.validator(eventWithInvalidStateContext),
      false
    );

    const eventWithMissingStateFields = {
      ...mockStepFunctionsEvent,
      stateContext: {
        Name: 'Invoke Lambda function',
      },
    };

    assert.strictEqual(
      StepFunctionsTrigger.validator(eventWithMissingStateFields),
      false
    );
  });

  it('should validate Step Functions event with invalid stateMachineContext', () => {
    const eventWithInvalidStateMachineContext = {
      ...mockStepFunctionsEvent,
      stateMachineContext: {
        Id: 123,
        Name: 'StateMachine-tYitg6Hm7nTT',
      },
    };

    assert.strictEqual(
      StepFunctionsTrigger.validator(eventWithInvalidStateMachineContext),
      false
    );

    const eventWithMissingStateMachineFields = {
      ...mockStepFunctionsEvent,
      stateMachineContext: {
        Id: 'arn:aws:states:eu-west-1:396913735602:stateMachine:StateMachine-tYitg6Hm7nTT',
      },
    };

    assert.strictEqual(
      StepFunctionsTrigger.validator(eventWithMissingStateMachineFields),
      false
    );
  });

  it('should reject Step Functions event without required contexts', () => {
    const eventWithoutStateContext = {
      input: {},
      executionContext: mockStepFunctionsEvent.executionContext,
      stateMachineContext: mockStepFunctionsEvent.stateMachineContext,
    };

    assert.strictEqual(
      StepFunctionsTrigger.validator(eventWithoutStateContext),
      false
    );

    const eventWithoutStateMachineContext = {
      input: {},
      executionContext: mockStepFunctionsEvent.executionContext,
      stateContext: mockStepFunctionsEvent.stateContext,
    };

    assert.strictEqual(
      StepFunctionsTrigger.validator(eventWithoutStateMachineContext),
      false
    );
  });

  it('should initialize Step Functions span correctly', () => {
    const result = StepFunctionsTrigger.initializer(mockStepFunctionsEvent);

    assert.strictEqual(result.name, 'StateMachine-tYitg6Hm7nTT');
    assert.strictEqual(result.origin, TriggerOrigin.STEP_FUNCTIONS);
    assert.strictEqual(result.options.kind, SpanKind.SERVER);

    const attributes = result.options.attributes!;
    assert.strictEqual(attributes['faas.trigger'], 'stepfunctions');
    assert.strictEqual(attributes['faas.trigger.type'], 'Step Functions');
    assert.strictEqual(
      attributes['aws.stepfunctions.execution.arn'],
      mockStepFunctionsEvent.executionContext.Id
    );
    assert.strictEqual(
      attributes['aws.stepfunctions.execution.name'],
      mockStepFunctionsEvent.executionContext.Name
    );
    assert.strictEqual(
      attributes['aws.stepfunctions.state_machine.name'],
      'StateMachine-tYitg6Hm7nTT'
    );
    assert.strictEqual(
      attributes['aws.stepfunctions.execution.type'],
      'express'
    );
    assert.strictEqual(
      attributes['aws.stepfunctions.role.arn'],
      mockStepFunctionsEvent.executionContext.RoleArn
    );
    assert.strictEqual(
      attributes['aws.stepfunctions.execution.start_time'],
      mockStepFunctionsEvent.executionContext.StartTime
    );
    assert.strictEqual(
      attributes['aws.stepfunctions.execution.redrive_count'],
      0
    );
    assert.strictEqual(attributes['cloud.region'], 'eu-west-1');
    assert.strictEqual(attributes['cloud.account.id'], '396913735602');

    // Verify state context attributes
    assert.strictEqual(
      attributes['aws.stepfunctions.state.name'],
      'Invoke Lambda function'
    );
    assert.strictEqual(
      attributes['aws.stepfunctions.state.entered_time'],
      '2025-08-19T14:54:45.858Z'
    );
    assert.strictEqual(attributes['aws.stepfunctions.state.retry_count'], 0);

    // Verify state machine context attributes
    assert.strictEqual(
      attributes['aws.stepfunctions.state_machine.arn'],
      'arn:aws:states:eu-west-1:396913735602:stateMachine:StateMachine-tYitg6Hm7nTT'
    );
    assert.strictEqual(
      attributes['aws.stepfunctions.state_machine.name'],
      'StateMachine-tYitg6Hm7nTT'
    );
  });

});
