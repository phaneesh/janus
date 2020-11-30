package io.raven.db.dao.testdata.entities;

import io.raven.db.annotations.LookupKey;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.Table;

@Entity
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
@Table(name = "relations")
public class RelationalEntity {

  @Id
  @GeneratedValue(strategy = GenerationType.IDENTITY)
  private long id;

  @LookupKey
  @Column(name = "key", nullable = false, unique = true)
  private String key;

  @Column(name = "value", nullable = false, unique = true)
  private String value;

}
