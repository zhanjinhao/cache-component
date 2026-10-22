package cn.addenda.component.cache;

/**
 * 缓存组件拒绝接受某个参数时抛出的统一异常。
 * <p>
 * <b>语义边界</b>：它表示"<b>本组件</b>不允许这个参数穿透到存储层"，而不是"服务端不接受参数"。
 * 服务端怎么想是存储层的事；调用方看到本类型时，应该理解为组件在它这里就拦下了这次调用
 * （或者把存储层的拒绝收敛成了组件自己的拒绝），存储层最终有没有收到、回了什么都不改变这个口径。
 * <p>
 * <b>两个执行点</b>：
 * <ul>
 *   <li><b>入口校验</b>：参数在到达存储层之前就被拦下 —— null key、容量 / threshold 越界、
 *       null delegate、keyPrefix / id 为 null 等；</li>
 *   <li><b>归一存储层拒绝</b>：参数已经下发给存储层，存储层拒绝后，实现把那个拒绝统一翻译成
 *       本类型 —— 目前只有 redis 的超大 TTL（{@code ERR invalid expire time}）。这类参数同样
 *       属于"组件不允许"，不能因为它恰好由服务端先看到就降级成别的语义。</li>
 * </ul>
 * <b>与 {@link CacheUnsupportedOperationException} 的分工</b>：本类型是"参数有问题"，
 * 后者是"操作在当前实现或当前服务端上整体不可用"（如只读外壳的写方法、redis 不支持
 * {@code computeIfAbsent}）。两者都是 {@link CacheException} 的子类，业务侧只处理这一棵继承树。
 * <p>
 * 统一了原先使用的 JDK {@link IllegalArgumentException}：它太通用，业务系统配全局异常处理器时
 * 无法判断错误是否来自本组件。
 *
 * @author addenda
 * @since 2026/10/19
 */
public class CacheIllegalArgumentException extends CacheException {

  public CacheIllegalArgumentException() {
    super();
  }

  public CacheIllegalArgumentException(String message) {
    super(message);
  }

  public CacheIllegalArgumentException(String message, Throwable cause) {
    super(message, cause);
  }

  public CacheIllegalArgumentException(Throwable cause) {
    super(cause);
  }

}
