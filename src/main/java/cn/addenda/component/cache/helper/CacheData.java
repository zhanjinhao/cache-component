package cn.addenda.component.cache.helper;

import cn.addenda.component.base.jackson.deserialzer.LocalDateTimeStrDeSerializer;
import cn.addenda.component.base.jackson.serialzer.LocalDateTimeStryMdHmsSerializer;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import lombok.*;

import java.time.LocalDateTime;

/**
 * @author addenda
 * @since 2023/05/30
 */
@Setter
@Getter
@ToString
@AllArgsConstructor
@NoArgsConstructor
public class CacheData<T> {

  @JsonSerialize(using = LocalDateTimeStryMdHmsSerializer.class)
  @JsonDeserialize(using = LocalDateTimeStrDeSerializer.class)
  private LocalDateTime expireTime;

  private T data;

  public CacheData(T data) {
    this.data = data;
  }

}
