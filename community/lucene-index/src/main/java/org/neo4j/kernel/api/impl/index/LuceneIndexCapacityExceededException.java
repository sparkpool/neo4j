package org.neo4j.kernel.api.impl.index;

import java.io.IOException;

class LuceneIndexCapacityExceededException extends IOException
{
    LuceneIndexCapacityExceededException( String message )
    {
        super( message );
    }
}
