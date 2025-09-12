package org.csu.sdolp.storage.index;

import org.csu.sdolp.common.model.RID;
import org.csu.sdolp.common.model.Value;

public record KeyValuePair(Value key, RID rid) {
}