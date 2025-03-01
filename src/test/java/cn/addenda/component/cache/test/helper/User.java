package cn.addenda.component.cache.test.helper;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

import java.time.LocalDateTime;

/**
 * @author addenda
 * @since 2023/3/8 15:21
 */
@Setter
@Getter
@ToString
public class User {

  private String userId;

  private String username;


  private LocalDateTime createTm;


  public static User newUser(String userId) {
    User user = new User();
    user.setUserId(userId);
    user.setUsername(userId + "姓名");
    user.setCreateTm(LocalDateTime.now());
    return user;
  }

}
