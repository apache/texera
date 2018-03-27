package edu.uci.ics.texera.dataflow.annotation;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * This annotation indicates that the operator property belongs to the advance options in the frontend.
 * The property will be hidden by default. 
 * 
 * @author zuozhiw
 *
 */
@Documented
@Target(ElementType.PARAMETER)
@Retention(RetentionPolicy.RUNTIME)
public @interface AdvancedOption {
    
    boolean isAdvancedOption() default true;

}
