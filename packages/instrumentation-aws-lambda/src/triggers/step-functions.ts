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

import { Attributes, SpanKind } from '@opentelemetry/api';
import { LambdaTrigger, TriggerSpanInitializerResult } from './common';
import { TriggerOrigin } from './index';

interface StepFunctionExecutionContext {
  Id: string;
  Input: any;
  Name: string;
  RoleArn: string;
  StartTime: string;
  RedriveCount: number;
}

interface StepFunctionStateContext {
  Name: string;
  EnteredTime: string;
  RetryCount: number;
}

interface StepFunctionStateMachineContext {
  Id: string;
  Name: string;
}

interface StepFunctionsEvent {
  input: any;
  executionContext: StepFunctionExecutionContext;
  stateContext: StepFunctionStateContext;
  stateMachineContext: StepFunctionStateMachineContext;
}

function isStepFunctionsEvent(event: any): event is StepFunctionsEvent {
  return (
    event &&
    typeof event === 'object' &&
    'executionContext' in event &&
    event.executionContext &&
    typeof event.executionContext === 'object' &&
    'Id' in event.executionContext &&
    'Name' in event.executionContext &&
    'RoleArn' in event.executionContext &&
    'StartTime' in event.executionContext &&
    typeof event.executionContext.Id === 'string' &&
    typeof event.executionContext.Name === 'string' &&
    typeof event.executionContext.RoleArn === 'string' &&
    typeof event.executionContext.StartTime === 'string' &&
    'stateContext' in event &&
    event.stateContext &&
    typeof event.stateContext === 'object' &&
    'Name' in event.stateContext &&
    'EnteredTime' in event.stateContext &&
    'RetryCount' in event.stateContext &&
    typeof event.stateContext.Name === 'string' &&
    typeof event.stateContext.EnteredTime === 'string' &&
    typeof event.stateContext.RetryCount === 'number' &&
    'stateMachineContext' in event &&
    event.stateMachineContext &&
    typeof event.stateMachineContext === 'object' &&
    'Id' in event.stateMachineContext &&
    'Name' in event.stateMachineContext &&
    typeof event.stateMachineContext.Id === 'string' &&
    typeof event.stateMachineContext.Name === 'string'
  );
}

function initializeStepFunctionsSpan(
  event: StepFunctionsEvent
): TriggerSpanInitializerResult {
  const { executionContext, stateContext, stateMachineContext } = event;

  // Extract state machine name from the execution ARN
  // Format: arn:aws:states:region:account:express:StateMachineName:executionName:taskToken
  const arnParts = executionContext.Id.split(':');
  const stateMachineName = arnParts.length >= 7 ? arnParts[6] : 'unknown';
  const region = arnParts.length >= 4 ? arnParts[3] : 'unknown';
  const accountId = arnParts.length >= 5 ? arnParts[4] : 'unknown';
  const executionType = arnParts.length >= 6 ? arnParts[5] : 'unknown'; // 'express' or 'standard'

  const attributes: Attributes = {
    'faas.trigger': 'stepfunctions',
    'faas.trigger.type': 'Step Functions',
    'aws.stepfunctions.execution.arn': executionContext.Id,
    'aws.stepfunctions.execution.name': executionContext.Name,
    'aws.stepfunctions.execution.type': executionType,
    'aws.stepfunctions.role.arn': executionContext.RoleArn,
    'aws.stepfunctions.execution.start_time': executionContext.StartTime,
    'aws.stepfunctions.execution.redrive_count': executionContext.RedriveCount,
    'cloud.region': region,
    'cloud.account.id': accountId,
    'aws.stepfunctions.state.name': stateContext.Name,
    'aws.stepfunctions.state.entered_time': stateContext.EnteredTime,
    'aws.stepfunctions.state.retry_count': stateContext.RetryCount,
    'aws.stepfunctions.state_machine.arn': stateMachineContext.Id,
    'aws.stepfunctions.state_machine.name': stateMachineName,
  };

  const name = `${stateContext.Name}`;
  const options = {
    kind: SpanKind.SERVER,
    attributes,
  };

  return { name, options, origin: TriggerOrigin.STEP_FUNCTIONS };
}

export const StepFunctionsTrigger: LambdaTrigger<StepFunctionsEvent> = {
  validator: isStepFunctionsEvent,
  initializer: initializeStepFunctionsSpan,
};
