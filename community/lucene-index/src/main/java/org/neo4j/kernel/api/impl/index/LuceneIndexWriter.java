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
import org.apache.lucene.index.IndexDeletionPolicy;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.SearcherFactory;
import org.apache.lucene.search.SearcherManager;
import org.apache.lucene.store.Directory;

import java.io.Closeable;
import java.io.IOException;
import java.util.Map;

abstract class LuceneIndexWriter implements Closeable
{
    private static final long MAX_DOC_LIMIT = Integer.MAX_VALUE - 2048;

    protected final IndexWriter writer;

    LuceneIndexWriter( Directory dir, IndexWriterConfig conf ) throws IOException
    {
        this.writer = new IndexWriter( dir, conf );
    }

    abstract void addDocument( Document document ) throws IOException;

    abstract void updateDocument( Term term, Document document ) throws IOException;

    abstract void deleteDocuments( Term term ) throws IOException;

    IndexDeletionPolicy getIndexDeletionPolicy()
    {
        return writer.getConfig().getIndexDeletionPolicy();
    }

    void commit() throws IOException
    {
        writer.commit();
    }

    void commit( Map<String,String> commitUserData ) throws IOException
    {
        writer.commit( commitUserData );
    }

    @Override
    public void close() throws IOException
    {
        close( true );
    }

    void close( boolean waitForMerges ) throws IOException
    {
        writer.close( waitForMerges );
    }

    SearcherManager createSearcherManager() throws IOException
    {
        return new SearcherManager( writer, true, new SearcherFactory() );
    }

    void reserveDocumentAdditions( int additionsCount ) throws IOException
    {
        throw new UnsupportedOperationException( "Should not be called" );
    }

    long maxDocLimit()
    {
        return MAX_DOC_LIMIT;
    }
}
