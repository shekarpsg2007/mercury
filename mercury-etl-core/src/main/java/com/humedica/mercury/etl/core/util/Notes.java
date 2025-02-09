package com.humedica.mercury.etl.core.util;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;

/**
 * This annotation type is intended to be used as a documentation supplement
 * for <code>com.humedica.mercury.etl.core.engine.EntitySource</code> implementations.
 *
 * Comments entered into instances of this annotation will appear as comments
 * generated by automatic documentation processes.
 */
@Retention(RetentionPolicy.RUNTIME)
public @interface Notes {
   String value() default "";

   String tables() default "";
   String columnSelect() default "";

   String beforeJoin() default "";
   String join() default "";
   String afterJoin() default "";

   String map() default "";
   String afterMap() default "";

   String beforeJoinExceptions() default "";
   String joinExceptions() default "";
   String afterJoinExceptions() default "";
   String mapExceptions() default "";
   String afterMapExceptions() default "";
}
