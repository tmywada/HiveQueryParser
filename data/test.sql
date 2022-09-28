
-----------------------------------------------------------------------------------------------------------------------------------------------------
---- 9999
---- AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA

create table schema.ta as
--
select 
		c4.cid, c4.cis, c4.dsc,  
		c4.ctcpty,
		c4.stts,
		c4.edts 
	from
	(
		select
			c3.cid, c3.cis, c3.dsc, 
			c3.ctcpty,
			c3.mstts,
			c3.edts,
			lag(c3.mstts) over (partition by c3.cid, c3.cis, c3.dsc order by c3.mstts) stts,
			rownumber%2 as rownumber_mod_2
		from
		(
			select 
				c2.cid, c2.cis, c2.dsc,
				c2.ctcpty,
				c2.mstts,
				c2.edts,
				ROW_NUMBER() over(partition by c2.cid, c2.cis, c2.dsc order by c2.mstts) rownumber
			from 	
			(
				select 
				distinct
					c1.cid, c1.cis, c1.dsc,
					c1.ctcpty,
					c1.edts,
					c1.mstts
					,if
					(
						c1.ctcpty = lag(c1.ctcpty) over (partition by c1.cid, c1.cis, c1.dsc order by c1.mstts),
						NULL, 'Y'
					) flag_wrt_begin_asc
					,if
					(
						c1.ctcpty = lag(c1.ctcpty) over (partition by c1.cid, c1.cis, c1.dsc order by c1.mstts desc),
						NULL, 'Y'
					) flag_wrt_begin_desc
				from 
					(
						select 
						distinct
							bcm.cid, bcm.cis, bcm.dsc,
							nvl(bcm.ctcpty,'NA') ctcpty,
							bcm.mstts,
							bcm.edts
						from 
							schama.bcc bcm
						where 
							cis != 'CCC' and ctcp_cd_nk = 'BBB'
					) c1
			) c2
			where 
				(
				(c2.flag_wrt_begin_asc = 'Y' and c2.flag_wrt_begin_desc is null)
				or (c2.flag_wrt_begin_desc = 'Y' and c2.flag_wrt_begin_asc is null)
				)
		) c3
	) c4
	where
		c4.rownumber_mod_2 = 0
	union all  ---$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$
		--- AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA
	select 
		c2.cid, c2.cis, c2.dsc,
		c2.ctcpty,
		c2.mstts stts,
		c2.edts cnt_cmptp_end_ts 
	from 	
	(
		select 
		distinct
			c1.cid, c1.cis, c1.dsc,
			c1.ctcpty,
			c1.edts,
			c1.mstts
			,if
			(
				c1.ctcpty = lag(c1.ctcpty) over (partition by c1.cid, c1.cis, c1.dsc order by c1.mstts),
				NULL, 'Y'
			) flag_wrt_begin_asc
			,if
			(
				c1.ctcpty = lag(c1.ctcpty) over (partition by c1.cid, c1.cis, c1.dsc order by c1.mstts desc),
				NULL, 'Y'
			) flag_wrt_begin_desc
		from 
			(
				select 
				distinct
					bcm.cid, bcm.cis, bcm.dsc,
					nvl(bcm.ctcpty,'NA') ctcpty,
					bcm.mstts,
					bcm.edts
				from 
					tenant_insurance_cdsaimrd.brg_cnt_component bcm
				where 
					cis != 'CCC' and ctcp_cd_nk = 'BBB'
			) c1
	) c2
	where 
	    (
			c2.flag_wrt_begin_asc = 'Y' and c2.flag_wrt_begin_desc = 'Y'
		)
