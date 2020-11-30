package io.raven.db.dao.locktest;


import io.raven.db.annotations.LookupKey;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.OneToMany;
import javax.persistence.Table;
import javax.persistence.UniqueConstraint;
import java.util.Set;

@Entity
@Table(name = "some_related_table", uniqueConstraints = {
    @UniqueConstraint(columnNames = "my_id")
})
@Data
@NoArgsConstructor
public class SomeRelatedLookupObject {

  @Id
  @GeneratedValue(strategy = GenerationType.IDENTITY)
  private long id;

  @LookupKey
  @Column(name = "my_id", unique = true)
  private String myId;

  @Column
  private String name;

  @Column(name = "related_object")
  @OneToMany(mappedBy = "object")
  private Set<SomeLookupObject> relatedObject;

  @Builder
  public SomeRelatedLookupObject(String myId, String name) {
    this.myId = myId;
    this.name = name;
  }
}
