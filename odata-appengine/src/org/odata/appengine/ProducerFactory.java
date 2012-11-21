package org.odata.appengine;

import java.util.Properties;

import org.odata4j.edm.EdmDataServices;
import org.odata4j.producer.ODataProducer;
import org.odata4j.producer.ODataProducerFactory;

public class ProducerFactory implements ODataProducerFactory {

	private static final String NAMESPACE = "Datastore";
	private static final String BUILDER_PROPNAME = "org.odata.appengine.metadata.builder";

	@Override
	public ODataProducer create(Properties properties) {
		try {
			String prop = System.getProperty(BUILDER_PROPNAME);
			Class<?> builderClass = Class.forName(prop);
			MetadataBuilder builder = (MetadataBuilder) builderClass.newInstance();
			EdmDataServices metadata = builder.buildMetadata(NAMESPACE);
			return new Producer(metadata);
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
			return null;
		} catch (InstantiationException e) {
			e.printStackTrace();
			return null;
		} catch (IllegalAccessException e) {
			e.printStackTrace();
			return null;
		}
	}
}
