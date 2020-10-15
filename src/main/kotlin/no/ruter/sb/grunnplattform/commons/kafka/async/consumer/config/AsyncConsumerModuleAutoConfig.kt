package no.ruter.sb.grunnplattform.commons.kafka.async.consumer.config

import org.springframework.beans.factory.annotation.Configurable
import org.springframework.boot.context.properties.ConfigurationPropertiesScan
import org.springframework.boot.context.properties.EnableConfigurationProperties
import org.springframework.context.annotation.ComponentScan

@ComponentScan(basePackages = ["no.ruter.sb.grunnplattform.commons.kafka.async.consumer"])
@Configurable
class AsyncConsumerModuleAutoConfig