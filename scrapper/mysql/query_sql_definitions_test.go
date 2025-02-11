package mysql

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func Test_removeDynamicPartsOfSql(t *testing.T) {

	tests := []struct {
		name string
		in   string
		out  string
	}{
		{
			"AUTO_INCREMENT=xxx",
			"CREATE TABLE `sales` (\n  `sale_id` int(11) NOT NULL AUTO_INCREMENT,\n  `customer` int(11) NOT NULL,\n  `invoice_total` decimal(10,2) NOT NULL,\n  `invoice_positions` int(11) DEFAULT NULL,\n  `sale_datetime` datetime NOT NULL DEFAULT current_timestamp(),\n  `sale_date` date NOT NULL DEFAULT current_timestamp(),\n  PRIMARY KEY (`sale_id`)\n) ENGINE=InnoDB AUTO_INCREMENT=4002 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci",
			"CREATE TABLE `sales` (\n  `sale_id` int(11) NOT NULL AUTO_INCREMENT,\n  `customer` int(11) NOT NULL,\n  `invoice_total` decimal(10,2) NOT NULL,\n  `invoice_positions` int(11) DEFAULT NULL,\n  `sale_datetime` datetime NOT NULL DEFAULT current_timestamp(),\n  `sale_date` date NOT NULL DEFAULT current_timestamp(),\n  PRIMARY KEY (`sale_id`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := removeDynamicPartsOfSql(tt.in)
			assert.Equal(t, tt.out, got)
		})
	}
}
