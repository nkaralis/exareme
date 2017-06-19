/** my addition */
package com.continuent.bristlecone.benchmark.db;

public class SqlDialectForExareme extends AbstractSqlDialect{
	
	public SqlDialectForExareme() {
	}
	
	/** Return Exareme driver. */
	public String getDriver()
    {
		return "madgik.exareme.jdbc.federated.AdpDriver";
    }
	
	/** Returns true if the JDBC URL looks like an Exareme URL. */
    public boolean supportsJdbcUrl(String url)
	{
    	return (url.startsWith("jdbc:fedadp"));
	}

}
