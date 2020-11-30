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

package io.raven.db.dao.locktest;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import io.raven.db.dao.BaseDaoTest;
import io.raven.db.dao.LookupDao;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.List;
import java.util.Optional;

/**
 * Test locking behavior
 */
public class LockTest extends BaseDaoTest {

  private LookupDao<SomeLookupObject> lookupDao;
  private LookupDao<SomeRelatedLookupObject> otherLookupDao;

  @Before
  public void before() {
    List<Class<?>> entities = ImmutableList.<Class<?>>builder()
        .add(SomeLookupObject.class, SomeRelatedLookupObject.class).build();
    setup(entities);
    lookupDao = new LookupDao<>(sessionFactory, SomeLookupObject.class);
    otherLookupDao = new LookupDao<>(sessionFactory, SomeRelatedLookupObject.class);
  }


  @Test
  public void testLocking() throws Exception {
    SomeLookupObject p1 = SomeLookupObject.builder()
        .myId("0")
        .name("Parent 1")
        .build();
    Optional<SomeLookupObject> saved = lookupDao.save(p1);
    lookupDao.lockAndGetExecutor(saved.get().getId())
        .filter(parent -> !Strings.isNullOrEmpty(parent.getName()))
        .save(otherLookupDao, parent -> SomeRelatedLookupObject.builder()
            .myId(parent.getMyId())
            .name("Hello")
            .build())
        .mutate(parent -> parent.setName("Changed"))
        .execute();

    Assert.assertEquals(p1.getMyId(), lookupDao.get(saved.get().getId()).get().getMyId());
    Assert.assertEquals("Changed", lookupDao.get(saved.get().getId()).get().getName());
    Assert.assertEquals("Hello", otherLookupDao.get(1L).get().getName());
  }

  @Test
  public void testSaveAndGetExecutor() throws Exception {
    SomeLookupObject p1 = SomeLookupObject.builder()
        .myId("0")
        .name("Parent 1")
        .build();
    SomeLookupObject saved = lookupDao.saveAndGetExecutor(p1)
        .execute();
    Assert.assertEquals(p1.getMyId(), saved.getMyId());
    Assert.assertEquals("Parent 1", saved.getName());
  }
//
//
//  @Test
//  public void testLockingWithShardId() throws Exception {
//    SomeLookupObject p1 = SomeLookupObject.builder()
//        .myId("0")
//        .name("Parent 1")
//        .build();
//    Optional<SomeLookupObject> saved = lookupDao.save(p1);
//
//    Map<Integer, List<SomeLookupObject>> groupedObjects = lookupDao.scatterGatherGrouped(DetachedCriteria.forClass(SomeLookupObject.class).add(Restrictions.eq("id", saved.get().getId())));
//
//    groupedObjects.forEach((shardId, ids) -> {
//      List<Long> longIds = ids.stream().map(id -> id.getId()).collect(Collectors.toList());
//      lookupDao.lockAndGetExecutor(shardId, longIds)
//          .save(relationDao, parent -> ImmutableList.of(SomeOtherObject.builder()
//              .my_id(parent.get(0).getMyId())
//              .value("Hello")
//              .build()))
//          .saveAll(relationDao,
//              parent -> IntStream.range(1, 6)
//                  .mapToObj(i -> SomeOtherObject.builder()
//                      .my_id(parent.get(0).getMyId())
//                      .value(String.format("Hello_%s", i))
//                      .build())
//                  .collect(Collectors.toList())
//          )
//          .mutate(parent -> parent.get(0).setName("Changed"))
//          .execute();
//
//    });
//
//    Assert.assertEquals(p1.getMyId(), lookupDao.get("0", saved.get().getId()).get().getMyId());
//    Assert.assertEquals("Changed", lookupDao.get("0", saved.get().getId()).get().getName());
//    System.out.println(relationDao.get("0", 1L).get());
//    Assert.assertEquals(6, relationDao.select("0", DetachedCriteria.forClass(SomeOtherObject.class)).size());
//    Assert.assertEquals("Hello", relationDao.get("0", 1L).get().getValue());
//  }
//
//
//  @Test
//  public void testLockingWithShardIdAndOtherLookupDao() throws Exception {
//    SomeLookupObject p1 = SomeLookupObject.builder()
//        .myId("0")
//        .name("Child 1")
//        .build();
//
//    SomeLookupObject p2 = SomeLookupObject.builder()
//        .myId("1")
//        .name("Child 2")
//        .build();
//    Optional<SomeLookupObject> saved1 = lookupDao.save(p2);
//    Optional<SomeLookupObject> saved = lookupDao.save(p1);
//
//    Map<Integer, List<SomeLookupObject>> groupedObjects = lookupDao.scatterGatherGrouped(DetachedCriteria.forClass(SomeLookupObject.class).add(Restrictions.in("myId", ImmutableList.of("1", "0"))));
//
//    groupedObjects.forEach((shardId, ids) -> {
//      List<Long> longIds = ids.stream().map(id -> id.getId()).collect(Collectors.toList());
//      lookupDao.lockAndGetExecutor(shardId, longIds)
//          .saveAllInShard(otherLookupDao, parent -> ImmutableList.of(SomeOtherRelatedLookupObject.builder()
//                  .myId(parent.get(0).getName()).name("Test").build()),
//              (otherRelatedObjects, otherObjects) -> {
//                otherObjects.forEach(otherObject -> otherObject.setObject(otherRelatedObjects.get(0)));
//                return null;
//              })
//          .mutate(parent -> parent.forEach(p -> p.setName("Changed")))
//          .execute();
//
//    });
//    Assert.assertEquals(p1.getMyId(), lookupDao.get("0", saved.get().getId()).get().getMyId());
//    Assert.assertNotNull(lookupDao.get("0", saved.get().getId()).get().getObject());
//    Assert.assertEquals(p2.getMyId(), lookupDao.get("1", saved1.get().getId()).get().getMyId());
//    Assert.assertNotNull(lookupDao.get("1", saved1.get().getId()).get().getObject());
//    Assert.assertEquals("Changed", lookupDao.get("0", saved.get().getId()).get().getName());
//  }
//
//
//  @Test
//  public void testLockingWithShardIdAndOtherLookupDaoAggregation() throws Exception {
//    SomeLookupObject p1 = SomeLookupObject.builder()
//        .myId("0")
//        .name("Child 1")
//        .build();
//
//    SomeLookupObject p2 = SomeLookupObject.builder()
//        .myId("1")
//        .name("Child 2")
//        .build();
//    Optional<SomeLookupObject> saved1 = lookupDao.save(p2);
//    Optional<SomeLookupObject> saved = lookupDao.save(p1);
//
//    Map<Integer, List<SomeLookupObject>> groupedObjects = lookupDao.scatterGatherGrouped(DetachedCriteria.forClass(SomeLookupObject.class).add(Restrictions.in("myId", ImmutableList.of("1", "0"))));
//
//    groupedObjects.forEach((shardId, ids) -> {
//      List<Long> longIds = ids.stream().map(id -> id.getId()).collect(Collectors.toList());
//      lookupDao.lockAndGetExecutor(shardId, longIds)
//          .saveAggregationInShard(otherLookupDao, parent -> Optional.of(SomeOtherRelatedLookupObject.builder()
//                  .myId(parent.get(0).getName()).name("Test").build()),
//              (otherRelatedObject, otherObjects) -> {
//                otherObjects.forEach(otherObject -> otherObject.setObject(otherRelatedObject.isPresent() ? otherRelatedObject.get() : null));
//                return null;
//              })
//          .mutate(parent -> parent.forEach(p -> p.setName("Changed")))
//          .execute();
//
//    });
//    Assert.assertEquals(p1.getMyId(), lookupDao.get("0", saved.get().getId()).get().getMyId());
//    Assert.assertNotNull(lookupDao.get("0", saved.get().getId()).get().getObject());
//    Assert.assertEquals(p2.getMyId(), lookupDao.get("1", saved1.get().getId()).get().getMyId());
//    Assert.assertNotNull(lookupDao.get("1", saved1.get().getId()).get().getObject());
//    Assert.assertEquals("Changed", lookupDao.get("0", saved.get().getId()).get().getName());
//  }
//
//
//  @Test
//  public void testLockingWithShardIdAndOtherLookupDaoAggregationNull() throws Exception {
//    SomeLookupObject p1 = SomeLookupObject.builder()
//        .myId("0")
//        .name("Child 1")
//        .build();
//
//    SomeLookupObject p2 = SomeLookupObject.builder()
//        .myId("1")
//        .name("Child 2")
//        .build();
//    Optional<SomeLookupObject> saved1 = lookupDao.save(p2);
//    Optional<SomeLookupObject> saved = lookupDao.save(p1);
//
//    Map<Integer, List<SomeLookupObject>> groupedObjects = lookupDao.scatterGatherGrouped(DetachedCriteria.forClass(SomeLookupObject.class).add(Restrictions.in("myId", ImmutableList.of("1", "0"))));
//
//    groupedObjects.forEach((shardId, ids) -> {
//      List<Long> longIds = ids.stream().map(SomeLookupObject::getId).collect(Collectors.toList());
//      lookupDao.lockAndGetExecutor(shardId, longIds)
//          .saveAggregationInShard(otherLookupDao, parent -> Optional.ofNullable(null),
//              (otherRelatedObject, otherObjects) -> {
//                otherObjects.forEach(otherObject -> {
//                  otherObject.setObject(otherRelatedObject.isPresent() ? otherRelatedObject.get() : null);
//                  otherObject.setName("Changed Child");
//                });
//                return null;
//              })
//          .execute();
//
//    });
//    Assert.assertEquals(p1.getMyId(), lookupDao.get("0", saved.get().getId()).get().getMyId());
//    Assert.assertEquals(p2.getMyId(), lookupDao.get("1", saved1.get().getId()).get().getMyId());
//    Assert.assertEquals("Changed Child", lookupDao.get("0", saved.get().getId()).get().getName());
//  }
//
//
//  @Test
//  public void testLockingWithShardIdAndOtherLookupDaoSingle() throws Exception {
//    SomeLookupObject p1 = SomeLookupObject.builder()
//        .myId("0")
//        .name("Child 3")
//        .build();
//
//    Optional<SomeLookupObject> saved = lookupDao.save(p1);
//
//    lookupDao.lockAndGetExecutor("0", saved.get().getId())
//        .saveInShard(otherLookupDao, parent -> SomeOtherRelatedLookupObject.builder()
//                .myId(parent.getName()).name("Test").build(),
//            (otherLookupObject, parent) -> {
//              parent.setObject(otherLookupObject);
//              return null;
//            })
//        .execute();
//
//    saved = lookupDao.get("0", saved.get().getId());
//    Optional<SomeOtherRelatedLookupObject> otherRelatedLookupObject = otherLookupDao.get("0", saved.get().getObject().getId());
//
//    assertTrue(otherRelatedLookupObject.isPresent());
//    Assert.assertSame("Test", otherRelatedLookupObject.get().getName());
//  }
//
//  @Test(expected = IllegalArgumentException.class)
//  public void testLockingFail() throws Exception {
//    SomeLookupObject p1 = SomeLookupObject.builder()
//        .myId("0")
//        .build();
//    Optional<SomeLookupObject> saved = lookupDao.save(p1);
//    System.out.println(lookupDao.get("0", saved.get().getId()).get().getName());
//    lookupDao.lockAndGetExecutor("0", saved.get().getId())
//        .filter(parent -> !Strings.isNullOrEmpty(parent.getName()))
//        .save(relationDao, parent -> {
//          SomeOtherObject result = SomeOtherObject.builder()
//              .my_id(parent.getMyId())
//              .value("Hello")
//              .build();
//          parent.setName("Changed");
//          return result;
//        })
//        .mutate(parent -> parent.setName("Changed"))
//        .execute();
//
//  }
//
//
//  @Test
//  public void testPersist() throws Exception {
//    SomeLookupObject p1 = SomeLookupObject.builder()
//        .myId("0")
//        .name("Parent 1")
//        .build();
//
//    SomeLookupObject saved = lookupDao.saveAndGetExecutor(p1)
//        .filter(parent -> !Strings.isNullOrEmpty(parent.getName()))
//        .save(relationDao, parent -> SomeOtherObject.builder()
//            .my_id(parent.getMyId())
//            .value("Hello")
//            .build())
//        .mutate(parent -> parent.setName("Changed"))
//        .execute();
//
//    Assert.assertEquals(p1.getMyId(), lookupDao.get("0", saved.getId()).get().getMyId());
//    Assert.assertEquals("Changed", lookupDao.get("0", saved.getId()).get().getName());
//  }
//
//  @Test
//  public void testUpdateById() throws Exception {
//    SomeLookupObject p1 = SomeLookupObject.builder()
//        .myId("0")
//        .name("Parent 1")
//        .build();
//
//    SomeOtherObject c1 = relationDao.save(p1.getMyId(), SomeOtherObject.builder()
//        .my_id(p1.getMyId())
//        .value("Hello")
//        .build()).get();
//
//
//    SomeLookupObject saved = lookupDao.saveAndGetExecutor(p1)
//        .filter(parent -> !Strings.isNullOrEmpty(parent.getName()))
//        .update(relationDao, c1.getId(), child -> {
//          child.setValue("Hello Changed");
//          return child;
//        })
//        .mutate(parent -> parent.setName("Changed"))
//        .execute();
//
//    Assert.assertEquals(p1.getMyId(), lookupDao.get("0", saved.getId()).get().getMyId());
//    Assert.assertEquals("Changed", lookupDao.get("0", saved.getId()).get().getName());
//    System.out.println(relationDao.get("0", 1L).get());
//    Assert.assertEquals("Hello Changed", relationDao.get("0", 1L).get().getValue());
//  }
//
//  @Test
//  public void testUpdateByEntity() throws Exception {
//    SomeLookupObject p1 = SomeLookupObject.builder()
//        .myId("0")
//        .name("Parent 1")
//        .build();
//
//    SomeOtherObject c1 = relationDao.save(p1.getMyId(), SomeOtherObject.builder()
//        .my_id(p1.getMyId())
//        .value("Hello")
//        .build()).get();
//
//
//    SomeLookupObject saved = lookupDao.saveAndGetExecutor(p1)
//        .filter(parent -> !Strings.isNullOrEmpty(parent.getName()))
//        .save(relationDao, c1, child -> {
//          child.setValue("Hello Changed");
//          return child;
//        })
//        .mutate(parent -> parent.setName("Changed"))
//        .execute();
//
//    Assert.assertEquals(p1.getMyId(), lookupDao.get("0", saved.getId()).get().getMyId());
//    Assert.assertEquals("Changed", lookupDao.get("0", saved.getId()).get().getName());
//    System.out.println(relationDao.get("0", 1L).get());
//    Assert.assertEquals("Hello Changed", relationDao.get("0", 1L).get().getValue());
//  }
//
//  @Test(expected = ConstraintViolationException.class)
//  public void testPersist_alreadyExistingDifferent() throws Exception {
//    SomeLookupObject p1 = SomeLookupObject.builder()
//        .myId("0")
//        .name("Parent 1")
//        .build();
//
//    lookupDao.save(p1);
//
//    SomeLookupObject p2 = SomeLookupObject.builder()
//        .myId("0")
//        .name("Changed")
//        .build();
//
//    SomeLookupObject saved = lookupDao.saveAndGetExecutor(p2)
//        .filter(parent -> !Strings.isNullOrEmpty(parent.getName()))
//        .save(relationDao, parent -> SomeOtherObject.builder()
//            .my_id(parent.getMyId())
//            .value("Hello")
//            .build())
//        .execute();
//
//    Assert.assertEquals(p1.getMyId(), lookupDao.get("0", saved.getId()).get().getMyId());
//    Assert.assertEquals("Changed", lookupDao.get("0", saved.getId()).get().getName());
//  }
//
//  @Test
//  public void testPersist_alreadyExistingSame() throws Exception {
//    SomeLookupObject p1 = SomeLookupObject.builder()
//        .myId("0")
//        .name("Parent 1")
//        .build();
//
//    lookupDao.save(p1);
//
//    SomeLookupObject saved = lookupDao.saveAndGetExecutor(p1)
//        .filter(parent -> !Strings.isNullOrEmpty(parent.getName()))
//        .save(relationDao, parent -> SomeOtherObject.builder()
//            .my_id(parent.getMyId())
//            .value("Hello")
//            .build())
//        .mutate(parent -> parent.setName("Changed"))
//        .execute();
//
//    Assert.assertEquals(p1.getMyId(), lookupDao.get("0", saved.getId()).get().getMyId());
//    Assert.assertEquals("Changed", lookupDao.get("0", saved.getId()).get().getName());
//  }
//
//
//
//  @Test
//  public void testLockAndGetBatch() throws Exception {
//    Optional<SomeLookupObject> one = lookupDao.save(SomeLookupObject.builder()
//        .myId("0")
//        .name("Parent 1")
//        .build());
//
//    assertTrue(one.isPresent());
//
//    BatchLockedContext<SomeLookupObject> context =
//    lookupDao.lockAndGetExecutor("0", () ->
//        Lists.newArrayList(one.get().getId()));
//    List<SomeLookupObject> savedEntities = context.save(relationDao, someLookupObjects ->
//      someLookupObjects.stream().map( e ->
//          SomeOtherObject.builder()
//            .my_id(e.getMyId()).value("Generated")
//            .build()).collect(Collectors.toList())
//    ).execute();
//    assertEquals(1, savedEntities.size());
//  }
//
//  @Test
//  public void testSaveAndGetBatch() throws Exception {
//    List<SomeLookupObject> savedEntities = lookupDao.saveAndGetExecutor("0",
//        Lists.newArrayList(SomeLookupObject.builder()
//            .myId("0")
//            .name("Parent 1")
//            .build()))
//        .save(relationDao, someLookupObjects ->
//            someLookupObjects.stream().map( e ->
//                SomeOtherObject.builder()
//                    .my_id(e.getMyId()).value("Generated")
//                    .build()).collect(Collectors.toList())
//        ).execute();
//    assertEquals(1, savedEntities.size());
//  }

  }
