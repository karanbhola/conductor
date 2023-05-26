package com.netflix.conductor.core.listener;

import org.springframework.amqp.core.Binding;
import org.springframework.amqp.core.BindingBuilder;
import org.springframework.amqp.core.DirectExchange;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.rabbit.batch.SimpleBatchingStrategy;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.core.BatchingRabbitTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;


@Configuration
public class QueueConfig {

	@Autowired
	private Environment env;

	@Value("${batching.strategy.batch-size}")
	private int batchSize;

	@Value("${batching.strategy.buffer-limit}")
	private int bufferLimit;

	@Value("${batching.strategy.timeout}")
	private int timeout;

	@Bean
	public Queue workflowListenerQueue(){
		return new Queue("workflow_listener_queue");
	}

	@Bean
	public DirectExchange exchange() {
		return new DirectExchange("direct-exchange");
	}

	@Bean
	Binding workflowListenerBinding(Queue workflowListenerQueue, DirectExchange exchange) {
		return BindingBuilder.bind(workflowListenerQueue).to(exchange).with("conductor_workflow_listener");
	}

	@Bean
	public BatchingRabbitTemplate batchingRabbitTemplate() {
		return new BatchingRabbitTemplate(connectionFactory(), new SimpleBatchingStrategy(batchSize, bufferLimit, timeout), null);
	}

	@Bean
	public ConnectionFactory connectionFactory() {
		CachingConnectionFactory connectionFactory = new CachingConnectionFactory();
		connectionFactory.setUri(env.getProperty("spring.rabbitmq.addresses"));
		connectionFactory.setUsername(env.getProperty("spring.rabbitmq.username"));
		connectionFactory.setPassword(env.getProperty("spring.rabbitmq.password"));
		return connectionFactory;
	}

}