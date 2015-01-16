/**
 * Copyright (c) 2002-2015 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.kernel.api.impl.index;

import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.Directory;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;

class LuceneIndexWriterForRuntime extends SimpleLuceneIndexWriter
{
    private final AtomicLong reservedDocs = new AtomicLong();

    LuceneIndexWriterForRuntime( Directory directory, IndexWriterConfig config ) throws IOException
    {
        super( directory, config );
    }

    @Override
    void addDocument( Document document ) throws IOException
    {
        super.addDocument( document );
        reservedDocs.decrementAndGet();
    }

    @Override
    synchronized void reserveDocumentAdditions( int additionsCount ) throws IOException
    {
        if ( totalNumberOfDocumentsExceededLuceneCapacity( additionsCount ) )
        {
            // maxDoc is about to overflow, let's try to save our index

            // try to merge deletes, maybe this will fix maxDoc
            writer.forceMergeDeletes();

            if ( totalNumberOfDocumentsExceededLuceneCapacity( additionsCount ) )
            {
                // if it did not then merge everything in single segment - horribly slow, last resort
                writer.forceMerge( 1 );
            }

            if ( totalNumberOfDocumentsExceededLuceneCapacity( additionsCount ) )
            {
                // merging did not help - throw exception
                throw new LuceneIndexCapacityExceededException(
                        "Unable to reserve " + additionsCount + " documents for insertion into index. " +
                        "Lucene index contains too many documents. Current limitation is " + maxDocLimit() + " " +
                        "documents per index.  Current value of maxDoc is " + writer.maxDoc() + "." );
            }
        }

        // everything fine - able to reserve 'space' for new documents
        reservedDocs.addAndGet( additionsCount );
    }

    private boolean totalNumberOfDocumentsExceededLuceneCapacity( int newAdditions )
    {
        return (reservedDocs.get() + writer.maxDoc() + newAdditions) > maxDocLimit();
    }
}
