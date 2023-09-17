package com.netflix.conductor.core.listener;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.core.DirectExchange;
import org.springframework.amqp.rabbit.core.BatchingRabbitTemplate;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.netflix.conductor.core.dal.ExecutionDAOFacade;
import com.netflix.conductor.metrics.Monitors;
import com.netflix.conductor.model.WorkflowModel;

/**
 * Provides implementation of workflow archiving immediately after workflow is completed or
 * terminated.
 *
 * @author karan.mohan.bhola
 */
@Component
public class AuthentricaWorkflowStatusListener implements WorkflowStatusListener {

	private static final Logger LOGGER = LoggerFactory.getLogger(AuthentricaWorkflowStatusListener.class);

	private final ExecutionDAOFacade executionDAOFacade;

	private final BatchingRabbitTemplate batchingRabbitTemplate;

	private final DirectExchange direct;

	private final ObjectMapper objectMapper;

	public AuthentricaWorkflowStatusListener(ExecutionDAOFacade executionDAOFacade, BatchingRabbitTemplate batchingRabbitTemplate, DirectExchange direct, ObjectMapper objectMapper) {
		this.executionDAOFacade = executionDAOFacade;
		this.batchingRabbitTemplate = batchingRabbitTemplate;
		this.direct = direct;
		this.objectMapper = objectMapper;
	}

	@Override
	public void onWorkflowCompleted(WorkflowModel workflow) {
		LOGGER.info("Workflow {} is completed", workflow.getWorkflowId());
		this.executionDAOFacade.removeWorkflow(workflow.getWorkflowId(), true);
		Monitors.recordWorkflowArchived(workflow.getWorkflowName(), workflow.getStatus());
	}

	@Override
	public void onWorkflowTerminated(WorkflowModel workflow) {
		LOGGER.info("Workflow {} is terminated", workflow.getWorkflowId());
		this.executionDAOFacade.removeWorkflow(workflow.getWorkflowId(), true);
		Monitors.recordWorkflowArchived(workflow.getWorkflowName(), workflow.getStatus());
	}

	@Override
	public void onWorkflowFinalized(WorkflowModel workflow) {
		LOGGER.info("Workflow {} is finalized", workflow.getWorkflowId());
		this.publishToWorkflowListenerQueue(workflow);
	}

	@Override
	public void onWorkflowCompletedIfEnabled(WorkflowModel workflow) {
		onWorkflowCompleted(workflow);
	}

	@Override
	public void onWorkflowTerminatedIfEnabled(WorkflowModel workflow) {
		onWorkflowTerminated(workflow);
	}

	@Override
	public void onWorkflowFinalizedIfEnabled(WorkflowModel workflow) {
		onWorkflowFinalized(workflow);
	}

	public void publishToWorkflowListenerQueue(WorkflowModel workflow) {
		LOGGER.info("sending workflow event to queue");
		try {
			String body = this.objectMapper.writeValueAsString(workflow);
			batchingRabbitTemplate.convertAndSend(this.direct.getName(), "conductor_workflow_listener", body);
			LOGGER.info("sent workflow event to queue");
		} catch (JsonProcessingException e) {
			LOGGER.error("error in sending workflow event to queue" + e.getLocalizedMessage());
		}
	}
}
