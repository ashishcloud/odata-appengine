package org.odata.appengine;

import java.util.Properties;

import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;

import org.odata4j.edm.EdmDataServices;
import org.odata4j.producer.ODataProducer;
import org.odata4j.producer.ODataProducerFactory;
import org.odata4j.producer.jpa.JPAEdmGenerator;

public class ProducerFactory implements ODataProducerFactory {

	private static final String PERSISTENCE_UNIT_NAME = "transactions-optional";
	private static final String NAMESPACE = "Datastore";

	@Override
	public ODataProducer create(Properties properties) {
		EntityManagerFactory emf = Persistence.createEntityManagerFactory(PERSISTENCE_UNIT_NAME);
		EdmDataServices metadata = new JPAEdmGenerator(emf, NAMESPACE).generateEdm(null).build();
		return new Producer(metadata);
	}
}
