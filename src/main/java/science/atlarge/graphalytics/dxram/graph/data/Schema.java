/*
 * Copyright (C) 2019 Heinrich-Heine-Universitaet Duesseldorf, Institute of Computer Science, Department Operating Systems
 *
 * This program is free software: you can redistribute it and/or modify it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>
 */

package science.atlarge.graphalytics.dxram.graph.data;

import java.util.ArrayList;
import java.util.List;

/**
 * @author Ruslan Curbanov, ruslan.curbanov@uni-duesseldorf.de, 05.03.2019
 *
 */
public final class Schema {

    private final List<SchemaEntry> m_schema;
    private int m_offset;

    public Schema() {
        m_schema = new ArrayList<SchemaEntry>();
        m_offset = 0;
    }

    public void addEntry(final Type p_type) {
        m_schema.add(new SchemaEntry(p_type, m_offset));
        m_offset += p_type.getSize();
    }

    public final class SchemaEntry {
 
        private final Type m_type;
        private final int m_offset;

        public SchemaEntry(final Type p_type, final int p_offset) {
            m_type = p_type;
            m_offset = p_offset;
        }

        public Type getType() {
            return m_type;
        }

        public int getOffset() {
            return m_offset;
        }
    }

    public final class Type {

        private final boolean m_primitive;
        private final PrimitiveType m_primitiveType;
        private final int m_arraySize;

        private Type(final PrimitiveType p_primitiveType) {
            this(p_primitiveType, -1);
        }

        private Type(final PrimitiveType p_primitiveType, final int p_arraySize) {
            m_primitive = true;
            m_primitiveType = p_primitiveType;
            m_arraySize = p_arraySize;
        }

        public boolean isPrimitive() {
            return m_primitive;
        }

        public boolean isArray() {
            return m_arraySize >= 0;
        }

        public PrimitiveType getPrimitiveType() {
            return m_primitiveType;
        }

        public int getSize() {
            if (m_primitive) {
                if (m_arraySize >=0) {
                    // primitive array
                    switch (m_primitiveType) {
                    case BOOL:
                    case BYTE:
                        return Integer.BYTES + (Byte.BYTES * m_arraySize);
                    case CHAR:
                        return Integer.BYTES + (Character.BYTES * m_arraySize);
                    case SHORT:
                        return Integer.BYTES + (Short.BYTES * m_arraySize);
                    case INT:
                        return Integer.BYTES + (Integer.BYTES * m_arraySize);
                    case FLOAT:
                        return Integer.BYTES + (Float.BYTES * m_arraySize);
                    case LONG:
                        return Integer.BYTES + (Long.BYTES * m_arraySize);
                    case DOUBLE:
                        return Integer.BYTES + (Double.BYTES * m_arraySize);
                    default:
                        return 0;
                    }
                } else {
                    // primitive type
                    switch (m_primitiveType) {
                    case BOOL:
                    case BYTE:
                        return Byte.BYTES;
                    case CHAR:
                        return Character.BYTES;
                    case SHORT:
                        return Short.BYTES;
                    case INT:
                        return Integer.BYTES;
                    case FLOAT:
                        return Float.BYTES;
                    case LONG:
                        return Long.BYTES;
                    case DOUBLE:
                        return Double.BYTES;
                    default:
                        return 0;
                    }
                }
            } else {
                // non-primitive TODO
                return 0;
            }
        }
    }

    public enum PrimitiveType {
        BOOL,
        BYTE,
        CHAR,
        DOUBLE,
        FLOAT,
        INT,
        LONG,
        SHORT
    }
}
