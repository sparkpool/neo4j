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

class LuceneIndexWriterForPopulation extends SimpleLuceneIndexWriter
{
    LuceneIndexWriterForPopulation( Directory directory, IndexWriterConfig config ) throws IOException
    {
        super( directory, config );
    }

    @Override
    void addDocument( Document document ) throws IOException
    {
        checkMaxDoc();
        super.addDocument( document );
    }

    private void checkMaxDoc() throws IOException
    {
        int currentMaxDoc = writer.maxDoc();
        if ( currentMaxDoc > maxDocLimit() )
        {
            throw new LuceneIndexCapacityExceededException(
                    "Lucene index contains too many documents. Current limitation is " + maxDocLimit() + " documents " +
                    "per index. Current value of maxDoc is " + currentMaxDoc + "." );
        }
    }
}