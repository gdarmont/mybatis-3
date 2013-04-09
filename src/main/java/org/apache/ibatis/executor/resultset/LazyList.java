package org.apache.ibatis.executor.resultset;

import java.sql.ResultSet;
import java.util.AbstractList;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

import org.apache.ibatis.mapping.ResultMap;

/**
 * @author GDA / GD06186S
 */
public class LazyList<E> extends AbstractList<E> {

  private final CursorList<E> cursorList;

  // Storage for already fetched elements
	private final ArrayList<E> storage = new ArrayList<E>();

  /**
   * Convenient constructor that takes a List, removing the need for an explicit cast.
   * But list implementation must be CursorList.
   *
   * @param cursorList
   * @throws IllegalArgumentException if list is not a CursorList
   */
  public LazyList(List<E> cursorList) {
    if (!(cursorList instanceof CursorList)) {
      throw new IllegalArgumentException("A CursorList is mandatory for LazyList");
    }
    this.cursorList = (CursorList) cursorList;
    checkForStartedCursor();
  }

  public LazyList(CursorList<E> cursorList) {
    this.cursorList = cursorList;
    checkForStartedCursor();
  }

  public LazyList(FastResultSetHandler resultSetHandler, ResultSet rs, ResultMap resultMap,
          FastResultSetHandler.ResultColumnCache resultColumnCache) {
    this(new CursorList<E>(resultSetHandler, rs, resultMap, resultColumnCache));
  }

  private void checkForStartedCursor() {
    if (cursorList.isFetchStarted()) {
      throw new IllegalStateException("Cannot use LazyList on a CursorList with already fetched items. "
              + "This would leads to a partial view of the result set.");
    }
  }

	@Override
	public E get(int index) {
    return getElementAtIndex(index);
  }

  /**
   * Retrieve elements from storage if available, or fetch from database.
   *
   * @param index index of the element to return
   * @return element at specified index
   * @throws IndexOutOfBoundsException if index is higher than elements count.
   */
  public E getElementAtIndex(int index) {
    if (index < storage.size()) {
      return storage.get(index);
    }

    E object = null;
    int currentIndex = storage.size() - 1;
    while (currentIndex < index) {
      object = cursorList.fetchNextObjectFromDatabase();
      currentIndex++;
      if (object != null) {
        storage.add(object);
      } else {
        throw new IndexOutOfBoundsException("Index: " + index + ", Size: " + storage.size());
      }
    }
    return object;
  }

  private void consumeCursor() {
    int currentIndex = storage.size();
    try {
      while (true) {
        getElementAtIndex(currentIndex++);
      }
    } catch (IndexOutOfBoundsException e) {
      // Ignore
    }
  }

  @Override
  public int size() {
    consumeCursor();
    return storage.size();
  }

  /**
   * This toString returns Object's toString default implementation since we don't want AbstractCollection#toString()
   * to iterate on collection.
   *
   * @return a string representation of the object.
   */
  @Override
  public String toString() {
    return getClass().getName() + "@" + Integer.toHexString(System.identityHashCode(this));
  }

  @Override
  public Iterator<E> iterator() {
    return new LazyIterator();
  }

  private class LazyIterator implements Iterator<E> {

    /**
     * Index of element to be returned by subsequent call to next.
     */
    int cursor = 0;
    /**
     * Holder for the next objet to be returned
     */
    E object;

    @Override
    public boolean hasNext() {
      try {
        object = getElementAtIndex(cursor);
      } catch (IndexOutOfBoundsException e) {
        object = null;
      }
      return object != null;
    }

    @Override
    public E next() {
      // Fill next with object fetched from hasNext()
      E next = object;

      if (next == null) {
        next = getElementAtIndex(cursor);
      }

      if (next != null) {
        object = null;
        cursor++;
        return next;
      }
      throw new NoSuchElementException();
    }

		@Override
		public void remove() {
			throw new UnsupportedOperationException("Cannot remove element from lazily loaded list");
		}
	}
}
