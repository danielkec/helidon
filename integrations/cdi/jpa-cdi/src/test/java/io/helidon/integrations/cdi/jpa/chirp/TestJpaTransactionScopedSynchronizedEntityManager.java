/*
 * Copyright (c) 2019 Oracle and/or its affiliates. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.helidon.integrations.cdi.jpa.chirp;

import java.util.Objects;

import javax.annotation.sql.DataSourceDefinition;
import javax.inject.Inject;
import javax.enterprise.context.ApplicationScoped;
import javax.enterprise.context.BeforeDestroyed;
import javax.enterprise.event.Observes;
import javax.enterprise.inject.se.SeContainer;
import javax.enterprise.inject.se.SeContainerInitializer;
import javax.persistence.EntityManager;
import javax.persistence.PersistenceContext;
import javax.persistence.PersistenceContextType;
import javax.persistence.SynchronizationType;
import javax.persistence.TransactionRequiredException;
import javax.transaction.HeuristicMixedException;
import javax.transaction.HeuristicRollbackException;
import javax.transaction.NotSupportedException;
import javax.transaction.RollbackException;
import javax.transaction.Status;
import javax.transaction.SystemException;
import javax.transaction.TransactionManager;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

@ApplicationScoped
@DataSourceDefinition(
    name = "chirp",
    className = "org.h2.jdbcx.JdbcDataSource",
    url = "jdbc:h2:mem:chirp;INIT=RUNSCRIPT FROM 'classpath:chirp.ddl'",
    serverName = "",
    properties = {
        "user=sa"
    }
)
class TestJpaTransactionScopedSynchronizedEntityManager {

    static {
        System.setProperty("jpaAnnotationRewritingEnabled", "true");
    }

    private SeContainer cdiContainer;

    @Inject
    private TransactionManager transactionManager;
    
    @PersistenceContext(
        type = PersistenceContextType.TRANSACTION,
        synchronization = SynchronizationType.SYNCHRONIZED,
        unitName = "chirp"
    )
    private EntityManager jpaTransactionScopedSynchronizedEntityManager;


    /*
     * Constructors.
     */


    TestJpaTransactionScopedSynchronizedEntityManager() {
        super();
    }


    /*
     * Setup and teardown methods.
     */


    @BeforeEach
    void startCdiContainer() {
        final SeContainerInitializer initializer = SeContainerInitializer.newInstance()
            .addBeanClasses(this.getClass());
        assertNotNull(initializer);
        this.cdiContainer = initializer.initialize();
    }

    @AfterEach
    void shutDownCdiContainer() {
        if (this.cdiContainer != null) {
            this.cdiContainer.close();
        }
    }


    /*
     * Support methods.
     */


    /**
     * A "business method" providing access to one of this {@link
     * TestJpaTransactionScopedEntityManager}'s {@link EntityManager}
     * instances for use by {@link Test}-annotated methods.
     *
     * @return a non-{@code null} {@link EntityManager}
     */
    EntityManager getJpaTransactionScopedSynchronizedEntityManager() {
        return this.jpaTransactionScopedSynchronizedEntityManager;
    }

    TransactionManager getTransactionManager() {
        return this.transactionManager;
    }

    private void onShutdown(@Observes @BeforeDestroyed(ApplicationScoped.class) final Object event,
                            final TransactionManager tm) throws SystemException {
        // If an assertion fails, or some other error happens in the
        // CDI container, there may be a current transaction that has
        // neither been committed nor rolled back.  Because the
        // Narayana transaction engine is fundamentally static, this
        // means that a transaction affiliation with the main thread
        // may "leak" into another JUnit test (since JUnit, by
        // default, forks once, and then runs all tests in the same
        // JVM).  CDI, thankfully, will fire an event for the
        // application context shutting down, even in the case of
        // errors.
        if (tm.getStatus() != Status.STATUS_NO_TRANSACTION) {
            tm.rollback();
        }
    }


    /*
     * Test methods.
     */


    @Test
    void testJpaTransactionScopedSynchronizedEntityManager()
        throws HeuristicMixedException,
               HeuristicRollbackException,
               NotSupportedException,
               RollbackException,
               SystemException
    {

        // Get a CDI contextual reference to this test instance.  It
        // is important to use "self" in this test instead of "this".
        final TestJpaTransactionScopedSynchronizedEntityManager self =
            this.cdiContainer.select(TestJpaTransactionScopedSynchronizedEntityManager.class).get();
        assertNotNull(self);

        // Get the EntityManager that is synchronized with and scoped
        // to a JTA transaction.
        final EntityManager em = self.getJpaTransactionScopedSynchronizedEntityManager();
        assertNotNull(em);
        assertTrue(em.isOpen());

        // We haven't started any kind of transaction yet and we
        // aren't testing anything using
        // the @javax.transaction.Transactional annotation so there is
        // no transaction in effect so the EntityManager cannot be
        // joined to one.
        assertFalse(em.isJoinedToTransaction());

        // Create a JPA entity and try to insert it.  This should fail
        // because according to JPA a TransactionRequiredException
        // will be thrown.
        final Author author = new Author("Abraham Lincoln");
        try {
            em.persist(author);
            fail("A TransactionRequiredException should have been thrown");
        } catch (final TransactionRequiredException expected) {
            
        }

        // Get the TransactionManager that normally is behind the
        // scenes and use it to start a Transaction.
        final TransactionManager tm = self.getTransactionManager();
        assertNotNull(tm);
        tm.begin();

        // Now magically our EntityManager should be joined to it.
        assertTrue(em.isJoinedToTransaction());

        // Roll the transaction back and note that our EntityManager
        // is no longer joined to it.
        tm.rollback();
        assertFalse(em.isJoinedToTransaction());

        // Start another transaction and persist our Author.
        tm.begin();
        em.persist(author);
        assertTrue(em.contains(author));
        tm.commit();

        // The transaction is over, so our EntityManager is not joined
        // to one anymore.
        assertFalse(em.isJoinedToTransaction());
        
        // Our PersistenceContextType is TRANSACTION, not EXTENDED, so
        // the underlying persistence context dies with the
        // transaction so the EntityManager's persistence context
        // should be empty, so the contains() operation should return
        // false.
        assertFalse(em.contains(author));
    }

}
