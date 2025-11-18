CREATE TABLE `export_history` (
	`id` varchar(36) NOT NULL DEFAULT UUID(),
	`dropshipper_email` text NOT NULL,
	`export_type` text NOT NULL,
	`date_range_from` timestamp,
	`date_range_to` timestamp,
	`payment_cycle_id` varchar(36),
	`total_records` int NOT NULL,
	`file_size` int,
	`exported_at` timestamp NOT NULL DEFAULT (now()),
	`export_params` json,
	CONSTRAINT `export_history_id` PRIMARY KEY(`id`)
);
--> statement-breakpoint
CREATE TABLE `order_data` (
	`id` varchar(36) NOT NULL DEFAULT UUID(),
	`upload_session_id` varchar(36) NOT NULL,
	`dropshipper_email` text NOT NULL,
	`order_id` text NOT NULL,
	`order_date` timestamp NOT NULL,
	`waybill` text,
	`product_name` text NOT NULL,
	`sku` text,
	`product_uid` text NOT NULL,
	`qty` int NOT NULL,
	`product_value` decimal(10,2) NOT NULL,
	`mode` text,
	`status` text NOT NULL,
	`delivered_date` timestamp,
	`rts_date` timestamp,
	`shipping_provider` text NOT NULL,
	`pincode` text,
	`state` text,
	`city` text,
	CONSTRAINT `order_data_id` PRIMARY KEY(`id`)
);
--> statement-breakpoint
CREATE TABLE `payment_cycles` (
	`id` varchar(36) NOT NULL DEFAULT UUID(),
	`dropshipper_email` text NOT NULL,
	`cycle_type` text NOT NULL,
	`cycle_params` json NOT NULL,
	`is_active` boolean NOT NULL DEFAULT true,
	`created_at` timestamp NOT NULL DEFAULT (now()),
	`updated_at` timestamp NOT NULL DEFAULT (now()),
	CONSTRAINT `payment_cycles_id` PRIMARY KEY(`id`)
);
--> statement-breakpoint
CREATE TABLE `payout_log` (
	`id` varchar(36) NOT NULL DEFAULT UUID(),
	`order_id` text NOT NULL,
	`waybill` text,
	`dropshipper_email` text NOT NULL,
	`product_uid` text NOT NULL,
	`paid_on` timestamp NOT NULL DEFAULT (now()),
	`period_from` timestamp NOT NULL,
	`period_to` timestamp NOT NULL,
	`paid_amount` decimal(10,2) NOT NULL,
	`payout_data` json,
	CONSTRAINT `payout_log_id` PRIMARY KEY(`id`)
);
--> statement-breakpoint
CREATE TABLE `product_prices` (
	`id` varchar(36) NOT NULL DEFAULT UUID(),
	`dropshipper_email` text NOT NULL,
	`product_uid` text NOT NULL,
	`product_name` text NOT NULL,
	`sku` text,
	`product_weight` decimal(8,3),
	`product_cost_per_unit` decimal(10,2) NOT NULL,
	`currency` text NOT NULL DEFAULT ('INR'),
	`updated_at` timestamp NOT NULL DEFAULT (now()),
	CONSTRAINT `product_prices_id` PRIMARY KEY(`id`)
);
--> statement-breakpoint
CREATE TABLE `rts_rto_reconciliation` (
	`id` varchar(36) NOT NULL DEFAULT UUID(),
	`order_id` text NOT NULL,
	`waybill` text,
	`dropshipper_email` text NOT NULL,
	`product_uid` text NOT NULL,
	`original_payout_id` varchar(36),
	`original_paid_amount` decimal(10,2) NOT NULL,
	`reversal_amount` decimal(10,2) NOT NULL,
	`rts_rto_status` text NOT NULL,
	`rts_rto_date` timestamp NOT NULL,
	`reconciled_on` timestamp NOT NULL DEFAULT (now()),
	`reconciled_by` text,
	`notes` text,
	`status` text NOT NULL DEFAULT ('pending'),
	CONSTRAINT `rts_rto_reconciliation_id` PRIMARY KEY(`id`)
);
--> statement-breakpoint
CREATE TABLE `settlement_exports` (
	`id` varchar(36) NOT NULL DEFAULT UUID(),
	`run_date` timestamp NOT NULL,
	`order_start` timestamp NOT NULL,
	`order_end` timestamp NOT NULL,
	`del_start` timestamp NOT NULL,
	`del_end` timestamp NOT NULL,
	`shipping_total` int NOT NULL,
	`cod_total` int NOT NULL,
	`product_cost_total` int NOT NULL,
	`adjustments_total` int NOT NULL DEFAULT 0,
	`final_payable` int NOT NULL,
	`orders_count` int NOT NULL,
	`exported_at` timestamp NOT NULL DEFAULT (now()),
	CONSTRAINT `settlement_exports_id` PRIMARY KEY(`id`)
);
--> statement-breakpoint
CREATE TABLE `settlement_settings` (
	`id` varchar(36) NOT NULL DEFAULT UUID(),
	`frequency` text NOT NULL,
	`last_payment_done_on` timestamp,
	`last_delivered_cutoff` timestamp,
	`d_plus_2_enabled` boolean NOT NULL DEFAULT true,
	`created_at` timestamp NOT NULL DEFAULT (now()),
	`updated_at` timestamp NOT NULL DEFAULT (now()),
	CONSTRAINT `settlement_settings_id` PRIMARY KEY(`id`)
);
--> statement-breakpoint
CREATE TABLE `shipping_rates` (
	`id` varchar(36) NOT NULL DEFAULT UUID(),
	`product_uid` text NOT NULL,
	`product_weight` decimal(8,3) NOT NULL,
	`shipping_provider` text NOT NULL,
	`shipping_rate_per_kg` decimal(10,2) NOT NULL,
	`currency` text NOT NULL DEFAULT ('INR'),
	`updated_at` timestamp NOT NULL DEFAULT (now()),
	CONSTRAINT `shipping_rates_id` PRIMARY KEY(`id`)
);
--> statement-breakpoint
CREATE TABLE `upload_sessions` (
	`id` varchar(36) NOT NULL DEFAULT UUID(),
	`filename` text NOT NULL,
	`total_rows` int NOT NULL,
	`processed_rows` int NOT NULL,
	`cancelled_rows` int NOT NULL,
	`uploaded_at` timestamp NOT NULL DEFAULT (now()),
	CONSTRAINT `upload_sessions_id` PRIMARY KEY(`id`)
);
--> statement-breakpoint
ALTER TABLE `export_history` ADD CONSTRAINT `export_history_payment_cycle_id_payment_cycles_id_fk` FOREIGN KEY (`payment_cycle_id`) REFERENCES `payment_cycles`(`id`) ON DELETE no action ON UPDATE no action;--> statement-breakpoint
ALTER TABLE `order_data` ADD CONSTRAINT `order_data_upload_session_id_upload_sessions_id_fk` FOREIGN KEY (`upload_session_id`) REFERENCES `upload_sessions`(`id`) ON DELETE no action ON UPDATE no action;--> statement-breakpoint
ALTER TABLE `rts_rto_reconciliation` ADD CONSTRAINT `rts_rto_reconciliation_original_payout_id_payout_log_id_fk` FOREIGN KEY (`original_payout_id`) REFERENCES `payout_log`(`id`) ON DELETE no action ON UPDATE no action;