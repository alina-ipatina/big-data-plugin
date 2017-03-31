/*! ******************************************************************************
 *
 * Pentaho Data Integration
 *
 * Copyright (C) 2002-2017 by Pentaho : http://www.pentaho.com
 *
 *******************************************************************************
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 ******************************************************************************/

package org.pentaho.di.concurrency;

import org.junit.Test;
import org.pentaho.amazon.AmazonS3FileSystemBootstrap;
import org.pentaho.di.core.vfs.KettleVFS;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

public class AmazonS3FileSystemBootstrapConcurrencyTest {

  @Test
  public void getAndAddSchemasConcurrently() throws Exception {
    final AtomicBoolean condition = new AtomicBoolean( true );
    final AmazonS3FileSystemBootstrap bootstrap = new AmazonS3FileSystemBootstrap();

    final int addersAmount = 1;
    final int gettersAmount = 1;

    List<Adder> adders = new ArrayList<>( addersAmount );
    for (int i = 0; i < addersAmount; i++) {
      adders.add( new Adder( bootstrap, condition ) );
    }

    List<Getter> getters = new ArrayList<>( gettersAmount );
    for (int i = 0; i < gettersAmount; i++) {
      getters.add( new Getter( condition ) );
    }

    ConcurrencyTestRunner.runAndCheckNoExceptionRaised( adders, getters, condition );
  }

  private class Getter extends StopOnErrorCallable<Object> {

    Getter( AtomicBoolean condition ) {
      super( condition );
    }

    @Override
    Object doCall() throws Exception {
      while ( condition.get() ) {
        KettleVFS.getInstance().getFileSystemManager().getSchemes();
      }
      return null;
    }
  }

  private class Adder extends StopOnErrorCallable<Object> {
    private final AmazonS3FileSystemBootstrap bootstrap;

    Adder( AmazonS3FileSystemBootstrap bootstrap, AtomicBoolean condition ) {
      super( condition );
      this.bootstrap = bootstrap;
    }

    @Override
    Object doCall() throws Exception {
      while ( condition.get() ) {
        bootstrap.onEnvironmentInit();
      }
      return null;
    }
  }
}
