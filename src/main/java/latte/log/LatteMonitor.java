/*-
 * #%L
 * com.oceanbase:latte-obkv-table-client
 * %%
 * Copyright (C) 2021 - 2024 OceanBase
 * %%
 * OBKV Table Client Framework is licensed under Mulan PSL v2.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *          http://license.coscl.org.cn/MulanPSL2
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 * #L%
 */

package latte.log;

import latte.lib.api.monitor.Monitor;
import latte.lib.api.monitor.Transaction;

public class LatteMonitor {
    static Monitor monitor = null;

    public static void setMonitor(Monitor monitor) {
        LatteMonitor.monitor = monitor;
    }

    public static Transaction getTransaction(String type) {
        if (monitor == null)
            return null;
        return monitor.getTransaction(type);
    }

}
