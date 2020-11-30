/*
 * Copyright 2016 Santanu Sinha <santanu.sinha@gmail.com>
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package io.raven.db.dao;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import io.raven.db.dao.testdata.entities.RelationalEntity;
import io.raven.db.dao.testdata.entities.TestEntity;
import org.hibernate.criterion.DetachedCriteria;
import org.hibernate.criterion.Restrictions;
import org.junit.Before;
import org.junit.Test;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class LookupDaoTest extends BaseDaoTest {

  private LookupDao<TestEntity> lookupDao;

  private LookupDao<RelationalEntity> otherLookupDao;

  @Before
  public void before() {
    List<Class<?>> entities = ImmutableList.<Class<?>>builder()
        .add(TestEntity.class, RelationalEntity.class).build();
    setup(entities);
    lookupDao = new LookupDao<>(sessionFactory, TestEntity.class);
    otherLookupDao = new LookupDao<>(sessionFactory, RelationalEntity.class);
  }

  @Test
  public void testEntityClass() {
    assertTrue(lookupDao.getEntityClass().isAssignableFrom(TestEntity.class));
  }

  @Test
  public void testSave() throws Exception {
    TestEntity testEntity = TestEntity.builder()
        .externalId("testId")
        .text("Some Text")
        .build();
    Optional<TestEntity> saved = lookupDao.save(testEntity);

    assertTrue(lookupDao.exists(saved.get().getId()));
    assertFalse(lookupDao.exists(100L));
    Optional<TestEntity> result = lookupDao.get(saved.get().getId());
    assertEquals("Some Text", result.get().getText());

    testEntity.setText("Some New Text");
    saved = lookupDao.save(testEntity);
    result = lookupDao.get(saved.get().getId());
    assertEquals("Some New Text", result.get().getText());

    boolean updateStatus = lookupDao.update(result.get().getId(), entity -> {
      if (entity.isPresent()) {
        TestEntity e = entity.get();
        e.setText("Updated text");
        return e;
      }
      return null;
    });

    assertTrue(updateStatus);
    result = lookupDao.get(saved.get().getId());
    assertEquals("Updated text", result.get().getText());

    updateStatus = lookupDao.update(result.get().getId(), entity -> {
      if (entity.isPresent()) {
        TestEntity e = entity.get();
        e.setText("Updated text");
        return e;
      }
      return null;
    });

    assertTrue(updateStatus);
  }


  @Test
  public void testSaveBatch() throws Exception {
    List<TestEntity> tobeSaved = new ArrayList<>();
    for (int i = 0; i < 10; i++) {
      TestEntity testEntity = TestEntity.builder()
          .externalId("testId")
          .text("Some Text " + i)
          .build();
      tobeSaved.add(testEntity);
    }
    List<TestEntity> saved = lookupDao.save(tobeSaved);
    List<TestEntity> fetched = lookupDao.select(DetachedCriteria.forClass(TestEntity.class)
        .add(Restrictions.eq("externalId", "testId")), e -> e);
    assertEquals(10, fetched.size());
  }

  @Test
  public void testCount() throws Exception {
    TestEntity testEntity = TestEntity.builder()
        .externalId("testId")
        .text("Some Text")
        .build();
    lookupDao.save(testEntity);
    long count = lookupDao.count(DetachedCriteria.forClass(TestEntity.class).add(Restrictions.eq("externalId", "testId")));
    assertEquals(1, count);
  }

  @Test
  public void testKeyField() {
    assertNotNull(lookupDao.getKeyField());
    assertEquals("externalId", lookupDao.getKeyField().getName());
  }

  @Test
  public void testIdField() {
    assertNotNull(lookupDao.getIdField());
    assertEquals("id", lookupDao.getIdField().getName());
  }

  @Test
  public void testLookup() throws Exception {
    TestEntity testEntity = TestEntity.builder()
        .externalId("lookupTest")
        .text("Some Text6666")
        .build();
    Optional<TestEntity> savedEntity = lookupDao.save(testEntity);
    assertTrue(savedEntity.isPresent());
    Optional<TestEntity> fetchedEntity = lookupDao.lookup("lookupTest");
    assertTrue(fetchedEntity.isPresent());
  }

  @Test
  public void testLookupMulti() throws Exception {
    lookupDao.save(TestEntity.builder()
        .externalId("lookupTestMulti")
        .text("Some Text6666")
        .build());
    lookupDao.save(TestEntity.builder()
        .externalId("lookupTestMulti")
        .text("Some Text6667")
        .build());
    List<TestEntity> fetchedEntity = lookupDao.lookupMulti("lookupTestMulti");
    assertEquals(2, fetchedEntity.size());
  }

  @Test
  public void testSelectSingle() throws Exception {
    TestEntity testEntity = TestEntity.builder()
        .externalId("getInShard0")
        .text("Some Text2")
        .build();
    Optional<TestEntity> savedEntity = lookupDao.save(testEntity);
    assertTrue(savedEntity.isPresent());
    Optional<TestEntity> fetched = lookupDao.selectSingle(DetachedCriteria.forClass(TestEntity.class)
        .add(Restrictions.eq("text", "Some Text2")), t -> t);
    assertTrue(fetched.isPresent());
  }

  @Test
  public void testSelect() throws Exception {
    TestEntity testEntity = TestEntity.builder()
        .externalId("getInShard0")
        .text("Some Text5")
        .build();
    Optional<TestEntity> savedEntity = lookupDao.save(testEntity);
    assertTrue(savedEntity.isPresent());
    List<TestEntity> fetched = lookupDao.select(
        DetachedCriteria.forClass(TestEntity.class).add(Restrictions.eq("text", "Some Text5")));
    assertFalse(fetched.isEmpty());
  }

  @Test
  public void testGetMulti() throws Exception {
    TestEntity testEntity = TestEntity.builder()
        .externalId("getInShard0")
        .text("Some Text5")
        .build();
    Optional<TestEntity> saved = lookupDao.save(testEntity);
    assertTrue(saved.isPresent());
    List<TestEntity> entities = lookupDao.get(Lists.newArrayList(saved.get().getId()));
    assertEquals(1, entities.size());
  }

  @Test
  public void testUpdateInLock() throws Exception {
    TestEntity testEntity = TestEntity.builder()
        .externalId("getInShard0")
        .text("Some Text5")
        .build();
    Optional<TestEntity> saved = lookupDao.save(testEntity);
    assertTrue(saved.isPresent());
    boolean result = lookupDao.updateInLock(saved.get().getId(),
        testEntity1 -> {
          testEntity1.ifPresent(entity -> entity.setText("updated"));
          return testEntity1.orElseGet(null);
        });
    assertTrue(result);
    Optional<TestEntity> entity = lookupDao.get(saved.get().getId());
    assertTrue(entity.isPresent());
    assertEquals("updated", entity.get().getText());
  }

  @Test
  public void testNoUpdate() throws Exception {
    TestEntity testEntity = TestEntity.builder()
        .externalId("getInShard0")
        .text("Some Text99")
        .build();
    Optional<TestEntity> saved = lookupDao.save(testEntity);
    assertTrue(saved.isPresent());
    boolean result = lookupDao.updateInLock(-1L,
        testEntity1 -> {
          testEntity1.ifPresent(entity -> entity.setText("updated"));
          return testEntity1.orElseGet(null);
        });
    assertFalse(result);
    Optional<TestEntity> entity = lookupDao.get(saved.get().getId());
    assertTrue(entity.isPresent());
    assertEquals("Some Text99", entity.get().getText());
  }

  @Test
  public void testNullUpdate() throws Exception {
    TestEntity testEntity = TestEntity.builder()
        .externalId("getInShard0")
        .text("Some Text99")
        .build();
    Optional<TestEntity> saved = lookupDao.save(testEntity);
    assertTrue(saved.isPresent());
    boolean result = lookupDao.updateInLock(saved.get().getId(),
        testEntity1 -> null);
    assertFalse(result);
    Optional<TestEntity> entity = lookupDao.get(saved.get().getId());
    assertTrue(entity.isPresent());
    assertEquals("Some Text99", entity.get().getText());
  }

  @Test
  public void testUpdateQuery() throws Exception {
    TestEntity testEntity = TestEntity.builder()
        .externalId("getInShard0")
        .text("Some Text99")
        .build();
    Optional<TestEntity> saved = lookupDao.save(testEntity);
    assertTrue(saved.isPresent());
    int updated = lookupDao.update("update TestEntity t set t.text=:text where t.id=:id", ImmutableMap.<String, Object>builder()
        .put("text", "Changed Text99")
        .put("id", saved.get().getId())
    .build());
    assertEquals(1, updated);
    Optional<TestEntity> entity = lookupDao.get(saved.get().getId());
    assertTrue(entity.isPresent());
    assertEquals("Changed Text99", entity.get().getText());
  }


  @Test
  public void testUpdateNativeQuery() throws Exception {
    TestEntity testEntity = TestEntity.builder()
        .externalId("getInShard0")
        .text("Some Text99")
        .build();
    Optional<TestEntity> saved = lookupDao.save(testEntity);
    assertTrue(saved.isPresent());
    int updated = lookupDao.updateNative("update test_entity t set t.text=:text where t.id=:id", ImmutableMap.<String, Object>builder()
        .put("text", "Changed Text99")
        .put("id", saved.get().getId())
        .build());
    assertEquals(1, updated);
    Optional<TestEntity> entity = lookupDao.get(saved.get().getId());
    assertTrue(entity.isPresent());
    assertEquals("Changed Text99", entity.get().getText());
  }

  @Test
  public void testSelectPaginated() throws Exception {
    TestEntity testEntity = TestEntity.builder()
        .externalId("getInShard0")
        .text("Some Text6")
        .build();
    Optional<TestEntity> saved = lookupDao.save(testEntity);
    assertTrue(saved.isPresent());
    List<TestEntity> entities = lookupDao.selectPaginated(DetachedCriteria.forClass(TestEntity.class), t -> t, 10);
    assertEquals(1, entities.size());
  }

  @Test
  public void testSelectLimitAndOffset() throws Exception {
    TestEntity testEntity = TestEntity.builder()
        .externalId("getInShard0")
        .text("Some Text777")
        .build();
    Optional<TestEntity> saved = lookupDao.save(testEntity);
    assertTrue(saved.isPresent());
    List<TestEntity> entities = lookupDao.select(DetachedCriteria.forClass(TestEntity.class), 10, 0);
    assertEquals(1, entities.size());
  }


  @Test
  public void testSum() throws Exception {
    TestEntity testEntity = TestEntity.builder()
        .externalId("getInShard0")
        .text("Some Text7")
        .amount(BigDecimal.TEN)
        .build();
    Optional<TestEntity> saved = lookupDao.save(testEntity);
    assertTrue(saved.isPresent());
    BigDecimal result = lookupDao.sum(DetachedCriteria.forClass(TestEntity.class), "amount");
    assertEquals(0, BigDecimal.TEN.compareTo(result));
  }

  @Test
  public void testSumZero() throws Exception {
    assertNull(lookupDao.sum(DetachedCriteria.forClass(TestEntity.class), "amount"));
  }

  @Test
  public void testExists() throws Exception {
    TestEntity testEntity = TestEntity.builder()
        .externalId("getInShard0")
        .text("Some Text98")
        .amount(BigDecimal.TEN)
        .build();
    Optional<TestEntity> saved = lookupDao.save(testEntity);
    assertTrue(saved.isPresent());
    assertTrue(lookupDao.exists(saved.get().getId()));
  }

  @Test
  public void testExistsKey() throws Exception {
    TestEntity testEntity = TestEntity.builder()
        .externalId("getInShard0")
        .text("Some Text99")
        .amount(BigDecimal.TEN)
        .build();
    Optional<TestEntity> saved = lookupDao.save(testEntity);
    assertTrue(saved.isPresent());
    assertTrue(lookupDao.exists("getInShard0"));
  }

  @Test
  public void testNotExists() throws Exception {
    assertFalse(lookupDao.exists(-1L));
  }

  @Test
  public void testMax() throws Exception {
    lookupDao.save(TestEntity.builder()
        .externalId("getInShard1")
        .text("Some Text99")
        .amount(BigDecimal.TEN)
        .build());
    lookupDao.save(TestEntity.builder()
        .externalId("getInShard1")
        .text("Some Text99")
        .amount(BigDecimal.ONE)
        .build());
    assertEquals(0, lookupDao.<BigDecimal>max(DetachedCriteria.forClass(TestEntity.class), "amount").compareTo(BigDecimal.TEN));
  }

  @Test
  public void testMin() throws Exception {
    lookupDao.save(TestEntity.builder()
        .externalId("getInShard1")
        .text("Some Text100")
        .amount(BigDecimal.TEN)
        .build());
    lookupDao.save(TestEntity.builder()
        .externalId("getInShard1")
        .text("Some Text101")
        .amount(BigDecimal.ONE)
        .build());
    assertEquals(0, lookupDao.<BigDecimal>min(DetachedCriteria.forClass(TestEntity.class), "amount").compareTo(BigDecimal.ONE));
  }

  @Test
  public void testSaveAll() throws Exception {
    Optional<TestEntity> saved = lookupDao.save(TestEntity.builder()
        .externalId("getInShard1")
        .text("Some Text101")
        .amount(BigDecimal.TEN)
        .build());
    assertTrue(saved.isPresent());
    lookupDao.lockAndGetExecutor(saved.get().getId())
        .saveAll(otherLookupDao, parent ->
          Collections.singletonList(RelationalEntity.builder()
            .key("test")
            .value("value")
            .build())
        )
        .execute();
    Optional<RelationalEntity> fetched = otherLookupDao.lookup("test");
    assertTrue(fetched.isPresent());
  }

  @Test
  public void testLockAndUpdate() throws Exception {
    Optional<TestEntity> saved = lookupDao.save(TestEntity.builder()
        .externalId("getInShard1")
        .text("Some Text101")
        .amount(BigDecimal.TEN)
        .build());
    assertTrue(saved.isPresent());
    Optional<RelationalEntity> savedRelated = otherLookupDao.save(RelationalEntity.builder()
        .key("test")
        .value("value")
        .build());

    lookupDao.lockAndGetExecutor(saved.get().getId())
        .update(otherLookupDao, savedRelated.get().getId(), otherEntity -> {
          if(otherEntity.isPresent()) {
            RelationalEntity updated = otherEntity.get();
            updated.setValue("updated");
            return updated;
          }
          return null;
        })
        .execute();
    Optional<RelationalEntity> fetched = otherLookupDao.lookup("test");
    assertEquals("updated", fetched.get().getValue());
  }

  @Test
  public void testLockAndUpdateWithQuery() throws Exception {
    Optional<TestEntity> saved = lookupDao.save(TestEntity.builder()
        .externalId("getInShard1")
        .text("Some Text102")
        .amount(BigDecimal.TEN)
        .build());
    assertTrue(saved.isPresent());
    Optional<RelationalEntity> savedRelated = otherLookupDao.save(RelationalEntity.builder()
        .key("test")
        .value("value")
        .build());

    Map<String, Object> params = ImmutableMap.<String, Object>builder()
        .put("id", savedRelated.get().getId())
        .put("value", "updated")
        .build();
    lookupDao.lockAndGetExecutor(saved.get().getId())
        .update(otherLookupDao, "update RelationalEntity set value =:value where id=:id", params)
        .execute();
    Optional<RelationalEntity> fetched = otherLookupDao.lookup("test");
    assertEquals("updated", fetched.get().getValue());
  }

  @Test
  public void testSaveAllBatch() throws Exception {
    Optional<TestEntity> saved = lookupDao.save(TestEntity.builder()
        .externalId("getInShard1")
        .text("Some Text101")
        .amount(BigDecimal.TEN)
        .build());
    assertTrue(saved.isPresent());
    lookupDao.lockAndGetExecutor(Collections.singletonList(saved.get().getId()))
        .saveAll(otherLookupDao, parent ->
            Collections.singletonList(RelationalEntity.builder()
                .key("test")
                .value("value")
                .build()), (relationalEntities, testEntities) -> null
        )
        .execute();
    Optional<RelationalEntity> fetched = otherLookupDao.lookup("test");
    assertTrue(fetched.isPresent());
  }

  @Test
  public void testSaveSingleBatch() throws Exception {
    Optional<TestEntity> saved = lookupDao.save(TestEntity.builder()
        .externalId("getInShard1")
        .text("Some Text101")
        .amount(BigDecimal.TEN)
        .build());
    assertTrue(saved.isPresent());
    lookupDao.lockAndGetExecutor(Collections.singletonList(saved.get().getId()))
        .saveSingle(otherLookupDao, parent ->
            Optional.of(RelationalEntity.builder()
                .key("test")
                .value("value")
                .build()), (relationalEntity, testEntities) -> null
        )
        .execute();
    Optional<RelationalEntity> fetched = otherLookupDao.lookup("test");
    assertTrue(fetched.isPresent());
  }

  @Test
  public void testSaveAndGetExecutor() {
    List<TestEntity> saved = lookupDao.saveAndGetExecutor(Collections.singletonList(TestEntity.builder()
        .externalId("getInShard1")
        .text("Some Text101")
        .amount(BigDecimal.TEN)
        .build()))
        .execute();
    assertEquals(1, saved.size());
  }

  @Test
  public void testBatchMutate() throws Exception {
    Optional<TestEntity> saved = lookupDao.save(TestEntity.builder()
        .externalId("getInShard1")
        .text("Some Text101")
        .amount(BigDecimal.TEN)
        .build());
    lookupDao.lockAndGetExecutor(Collections.singletonList(saved.get().getId()))
        .mutate(parent -> {
          for(TestEntity e : parent) {
            e.setText("Some Updated 101");
          }
        }).execute();
    Optional<TestEntity> fetched = lookupDao.get(saved.get().getId());
    assertTrue(fetched.isPresent());
    assertEquals("Some Updated 101", fetched.get().getText());
  }

  @Test
  public void testSelectQuery() throws Exception {
    Optional<TestEntity> saved = lookupDao.save(TestEntity.builder()
        .externalId("getInShard1")
        .text("Some Text109")
        .amount(BigDecimal.TEN)
        .build());
    assertTrue(saved.isPresent());
    List<TestEntity> fetched = lookupDao.select("from TestEntity where text='Some Text109'", Collections.emptyMap(), false);
    assertEquals(1, fetched.size());
  }

}