/***************************** BEGIN LICENSE BLOCK ***************************

 The contents of this file are subject to the Mozilla Public License, v. 2.0.
 If a copy of the MPL was not distributed with this file, You can obtain one
 at http://mozilla.org/MPL/2.0/.

 Software distributed under the License is distributed on an "AS IS" basis,
 WITHOUT WARRANTY OF ANY KIND, either express or implied. See the License
 for the specific language governing rights and limitations under the License.
 ******************************* END LICENSE BLOCK ***************************/

package org.sensorhub.impl.datastore.postgis.store.obs;

import org.junit.Test;

import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class TestLazyTieredStreamLifecycle {

    @Test
    public void lazyStreamDefersOpenUntilIterationAndClosesDelegate() {
        AtomicInteger opened = new AtomicInteger();
        AtomicInteger closed = new AtomicInteger();

        Stream<Integer> unused = OscarTieredObsStore.lazyStream(() -> {
            opened.incrementAndGet();
            return Stream.of(1, 2, 3).onClose(closed::incrementAndGet);
        });
        assertEquals("Supplier must not execute before terminal iteration", 0, opened.get());
        unused.close();
        assertEquals("Closing before iteration must not open delegate", 0, opened.get());
        assertEquals(0, closed.get());

        try (Stream<Integer> active = OscarTieredObsStore.lazyStream(() -> {
            opened.incrementAndGet();
            return Stream.of(10, 20, 30).onClose(closed::incrementAndGet);
        })) {
            assertEquals(0, opened.get());
            Optional<Integer> first = active.findFirst();
            assertEquals(Optional.of(10), first);
            assertEquals(1, opened.get());
            assertEquals(0, closed.get());
        }
        assertEquals("Delegate onClose must fire when lazyStream closes", 1, closed.get());
    }

    @Test
    public void mergeSortedDefersColdTierOpenUntilIterationStarts() {
        AtomicBoolean hotOpened = new AtomicBoolean();
        AtomicBoolean hotClosed = new AtomicBoolean();
        AtomicBoolean coldOpened = new AtomicBoolean();
        AtomicBoolean coldClosed = new AtomicBoolean();

        Stream<Integer> hot = OscarTieredObsStore.lazyStream(() -> {
            hotOpened.set(true);
            return Stream.of(1, 3).onClose(() -> hotClosed.set(true));
        });
        Stream<Integer> cold = OscarTieredObsStore.lazyStream(() -> {
            coldOpened.set(true);
            return Stream.of(2, 4).onClose(() -> coldClosed.set(true));
        });

        Stream<Integer> merged = OscarTieredObsStore.mergeSortedForTest(
                hot, cold, Comparator.naturalOrder());
        assertFalse("Hot stream must stay unopened during mergeSorted construction", hotOpened.get());
        assertFalse("Cold stream must stay unopened during mergeSorted construction", coldOpened.get());

        try (merged) {
            Optional<Integer> first = merged.findFirst();
            assertEquals(Optional.of(1), first);
            assertTrue(hotOpened.get());
            assertTrue(coldOpened.get());
        }
        assertTrue(hotClosed.get());
        assertTrue(coldClosed.get());
    }

    @Test
    public void limitAndCloseReleasesBothTiersImmediatelyWhenLimitIsReached() {
        AtomicBoolean hotOpened = new AtomicBoolean();
        AtomicBoolean hotClosed = new AtomicBoolean();
        AtomicBoolean coldOpened = new AtomicBoolean();
        AtomicBoolean coldClosed = new AtomicBoolean();

        Stream<Integer> hot = OscarTieredObsStore.lazyStream(() -> {
            hotOpened.set(true);
            return Stream.of(5, 10).onClose(() -> hotClosed.set(true));
        });
        Stream<Integer> cold = OscarTieredObsStore.lazyStream(() -> {
            coldOpened.set(true);
            return Stream.of(1, 20).onClose(() -> coldClosed.set(true));
        });

        Stream<Integer> bounded = OscarTieredObsStore.limitAndClose(
                OscarTieredObsStore.mergeSortedForTest(hot, cold, Comparator.naturalOrder()),
                1L);
        assertFalse("limitAndClose must not eagerly open upstream iterators", hotOpened.get());
        assertFalse("limitAndClose must not eagerly open cold cursor", coldOpened.get());

        List<Integer> emitted = bounded.toList();
        assertEquals(List.of(1), emitted);
        assertTrue("Hot stream must auto-close once limit=1 is reached", hotClosed.get());
        assertTrue("Cold stream must auto-close once limit=1 is reached", coldClosed.get());
    }
}
