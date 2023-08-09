/****** Object:  Table [dbo].[testcases_archive]    Script Date: 07.08.2023 02:09:10 ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE TABLE [dbo].[testcases_new](
	[id] [bigint] NOT NULL,
	[company] [bigint] NULL,
	[location] [bigint] NULL,
	[event] [bigint] NULL,
	[product] [bigint] NULL,
	[testuser] [bigint] NULL,
	[offer_id] [bigint] NULL,
	[invoice_id] [bigint] NULL,
	[laboratory_id] [bigint] NULL,
	[timeslot] [datetime2](7) NULL,
	[timeslot_end] [datetime2](7) NULL,
	[checkin_at] [datetime2](7) NULL,
	[test_date] [datetime2](7) NULL,
	[result_date] [datetime2](7) NULL,
	[consent_at] [datetime2](7) NULL,
	[status] [int] NULL,
	[created_at] [datetime2](7) NULL,
	[updated_at] [datetime2](7) NULL,
	[result] [varchar](50) NULL,
	[email_md5] [varchar](32) NULL
) ON [PRIMARY]
GO

--drop index IX_Id on testcases_new

create unique clustered index IX_Id on testcases_new (id) with (data_compression = page);

truncate table testcases_new

select count(*), max(id) from testcases_new
