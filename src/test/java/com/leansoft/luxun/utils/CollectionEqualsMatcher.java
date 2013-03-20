package com.leansoft.luxun.utils;


import static org.easymock.EasyMock.reportMatcher;

import java.util.Arrays;
import java.util.Collection;

import org.apache.commons.collections.CollectionUtils;
import org.easymock.EasyMock;
import org.easymock.IArgumentMatcher;

/**
 * Matches a collection supplied by the code to test against a given collection
 * and checks if they are equal.
 *  
 * @author anph
 * @see EasyMock#aryEq(Object[])
 * @see CollectionUtils#isEqualCollection(java.util.Collection, java.util.Collection)
 * @since 23 Jan 2009
 *
 */
public class CollectionEqualsMatcher implements IArgumentMatcher {
    private final Collection<?> expectedCollection;
    
    private CollectionEqualsMatcher(Collection<?> expectedCollection) {
        
        if (expectedCollection == null) {
            throw new IllegalArgumentException("Expected collection may not be null");
        }
        
        this.expectedCollection = expectedCollection;
    }
    
    /* (non-Javadoc)
     * @see org.easymock.IArgumentMatcher#appendTo(java.lang.StringBuffer)
     */
    public void appendTo(StringBuffer buffer) {
        buffer.append("colEq(").append(expectedCollection).append(")");
    }

    /* (non-Javadoc)
     * @see org.easymock.IArgumentMatcher#matches(java.lang.Object)
     */
    public boolean matches(Object collection) {
        /*
         * The expected collection is not null, so a null input should return false. Note that
         * instanceof will fail for null input.
         */
        return ((collection instanceof Collection<?>) 
                && CollectionUtils.isEqualCollection(expectedCollection, (Collection<?>) collection));
    }
    
    /**
     * Factory method to register a matcher which will compare the collection passed
     * by the code under test against the expected collection.
     * 
     * @param expectedCollection    the (non-<code>null</code>) collection expected
     * @return  the given expected collection
     * @see #colEq(Object...)
     */
    public static <T> Collection<T> colEq(Collection<T> expectedCollection) {
        reportMatcher(new CollectionEqualsMatcher(expectedCollection));
        return expectedCollection;
    }

    /**
     * Factory method to register a matcher which will compare the vararg collection passed
     * by the code under test against the expected collection.
     * 
     * @param expectedCollection    the collection expected
     * @return  the given expected collection
     * @see #colEq(Collection)
     */
    public static <T> Collection<T> colEq(T... expectedCollection) {
        return colEq(Arrays.asList(expectedCollection));
    }

}