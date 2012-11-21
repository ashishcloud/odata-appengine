package org.odata.appengine;

import org.odata4j.edm.EdmDataServices;

public interface MetadataBuilder {

	public abstract EdmDataServices buildMetadata(String namespace);
}
