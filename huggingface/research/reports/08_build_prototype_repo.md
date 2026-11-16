# 08 · Prototype Hugging Face repo (dry run)

Built from **171** sample athletes (3.7 GB of OSF zips) in 609 s → **1.7 GB** total (0.46× the zips), 418,776,629 time-series rows.

## Checks

| check | ok | detail |
|---|---|---|
| load_dataset('athletes') | ✅ | 171 rows, 11 columns |
| load_dataset('activities') | ✅ | 83,170 rows, 530 columns |
| load_dataset('raw_json') | ✅ | 171 rows, 2 columns |
| load_dataset('timeseries_exceptions') | ✅ | 9,214 rows, 6 columns |
| load_dataset('timeseries', streaming=True) | ✅ | {'athlete_id': '00ebaa97-f451-4fd6-bb05-f615021b0c89', 'activity_id': '2020_01_23_07_03_59', 'time_s': 0.0, 'distance_m' |
| join: activities.has_timeseries ⇔ timeseries | ✅ | missing=0, orphan=0 |
| athletes unique | ✅ |  |
| activities key unique | ✅ |  |
| timeseries exact at source-text level (all athletes, all values) | ✅ | 171 athletes, 2,512,650,560 values, 0 mismatches |
| timeseries_exceptions restore exact Float64 source values | ✅ | 9214 exception rows, 0 mismatches |
| duckdb glob query | ✅ | rows=418,776,629, athletes=169, max power=35890.0 |


## Repo tree

| path | size |
|---|---|
| README.md | 5.9 KB |
| data/activities/part-00000.parquet | 79.8 MB |
| data/athletes/athletes.parquet | 23.8 KB |
| data/raw_json/part-00000.parquet | 88.4 MB |
| data/timeseries/athlete_id=00ebaa97-f451-4fd6-bb05-f615021b0c89/part.parquet | 86.4 KB |
| data/timeseries/athlete_id=00ed6429-8e65-4be3-bd4c-8838cebada23/part.parquet | 1.8 MB |
| data/timeseries/athlete_id=01827dcf-3c84-455a-8be8-b1b3fcecb1e4/part.parquet | 1.6 MB |
| data/timeseries/athlete_id=01a599a0-f22d-4726-b81b-f2d7f3d9b559/part.parquet | 3.8 MB |
| data/timeseries/athlete_id=01ab1155-2f8d-4731-bef1-1b028303e91b/part.parquet | 2.9 MB |
| data/timeseries/athlete_id=01d9343b-50bf-480b-b06c-dfba4bdb1ca8/part.parquet | 2.5 MB |
| data/timeseries/athlete_id=02533c05-2edd-42f8-bb1d-20984d1434e6/part.parquet | 2.3 MB |
| data/timeseries/athlete_id=02a20442-5e89-476c-b863-1fb89f7b13e9/part.parquet | 2.9 MB |
| data/timeseries/athlete_id=060b0c4f-ab93-40c2-a79e-feb0422a7664/part.parquet | 8.9 MB |
| data/timeseries/athlete_id=0826999b-90ce-4a64-b002-b2186e2644c3/part.parquet | 2.1 MB |
| data/timeseries/athlete_id=08d6a74d-209d-4950-ab07-08f678ace554/part.parquet | 2.5 MB |
| data/timeseries/athlete_id=09c8e31d-26bd-45e4-97f3-a6b1efa23747/part.parquet | 444.1 KB |
| data/timeseries/athlete_id=0a1078fe-970a-4350-9b94-b3ce1f5a0a40/part.parquet | 12.1 MB |
| data/timeseries/athlete_id=0b03c530-4d65-4f18-a438-ba48dae0f379/part.parquet | 4.5 MB |
| data/timeseries/athlete_id=0ccc0b20-5eb1-4861-9aa3-605babf5b34e/part.parquet | 156.8 KB |
| data/timeseries/athlete_id=0f6b24ce-5c0e-4402-a15f-8b2a99d7325e/part.parquet | 131.5 KB |
| data/timeseries/athlete_id=0fedefa4-a443-4dc8-9c74-63e6ddc44fc2/part.parquet | 5.5 MB |
| data/timeseries/athlete_id=1004e532-2dc3-420d-b692-5ed4906b2488/part.parquet | 6.8 MB |
| data/timeseries/athlete_id=15162ffc-708a-48ad-a46c-4eaba802f0c0/part.parquet | 4.4 MB |
| data/timeseries/athlete_id=152a36b6-b537-4501-9a8c-03be00e7a5e5/part.parquet | 5.3 MB |
| data/timeseries/athlete_id=1559897e-c7a5-4fff-b229-ef77988dd5d1/part.parquet | 425.4 KB |
| data/timeseries/athlete_id=18cd3378-04d3-4fd4-abd9-ee2e45e9b57c/part.parquet | 1.3 MB |
| data/timeseries/athlete_id=1aaff9d4-5eee-4694-ad9d-77b7cd967733/part.parquet | 107.7 KB |
| data/timeseries/athlete_id=20543d1c-47fd-43e9-8bea-fdf25b46366a/part.parquet | 7.4 MB |
| data/timeseries/athlete_id=20b26b3a-e04c-4e01-993e-176f05d66b50/part.parquet | 304.9 KB |
| data/timeseries/athlete_id=233c7852-dd73-4d72-9d5c-324758654770/part.parquet | 5.5 MB |
| data/timeseries/athlete_id=23492c76-0536-43bd-9093-f29f9663dda1/part.parquet | 2.6 MB |
| data/timeseries/athlete_id=2351175b-97e2-46b3-80be-01a26ef0afd4/part.parquet | 1.5 MB |
| data/timeseries/athlete_id=24336b2f-e553-4062-9414-c80cf9ba0ba2/part.parquet | 238.3 KB |
| data/timeseries/athlete_id=255352f9-2efe-419e-98a8-4e147916a511/part.parquet | 21.5 MB |
| data/timeseries/athlete_id=25b00aae-eeaf-486f-bc1c-c63349a5bfc5/part.parquet | 1.4 MB |
| data/timeseries/athlete_id=26c20987-a0a6-4747-9e33-50198f6813a1/part.parquet | 4.5 MB |
| data/timeseries/athlete_id=2ab8d935-396d-4fe9-99e2-97b516d61825/part.parquet | 10.7 MB |
| data/timeseries/athlete_id=2f4929c9-5cc9-40d8-9be6-ce426eb6116a/part.parquet | 7.2 MB |
| data/timeseries/athlete_id=2f57cd13-8a28-497d-8a04-d872b4e3b6f0/part.parquet | 2.5 MB |
| data/timeseries/athlete_id=30985a92-bf47-4917-844f-e74221e6a7d1/part.parquet | 5.8 MB |
| data/timeseries/athlete_id=313d4918-9194-434f-9689-46e70047400b/part.parquet | 539.6 KB |
| data/timeseries/athlete_id=336d747a-c4a6-4d45-ae63-dd09588c9869/part.parquet | 231.3 KB |
| data/timeseries/athlete_id=347c8f92-0469-413a-903a-9c05fa96e52d/part.parquet | 101.1 MB |
| data/timeseries/athlete_id=34ac48d2-1b51-4d72-a71c-576e48e59a39/part.parquet | 609.0 KB |
| data/timeseries/athlete_id=34f3789f-d9cb-4151-b2db-2ac310ecfced/part.parquet | 620.3 KB |
| data/timeseries/athlete_id=373f4395-8166-4d69-ae74-dd96194a72ad/part.parquet | 22.6 MB |
| data/timeseries/athlete_id=3775a9f5-db6a-45cf-b3ff-1f7921bf007d/part.parquet | 26.3 MB |
| data/timeseries/athlete_id=38ec277a-e5a4-483c-9d59-6af0ff6bccab/part.parquet | 29.6 MB |
| data/timeseries/athlete_id=3a8de7bb-b71d-4444-95c9-8c2c8b10f8c3/part.parquet | 1.5 MB |
| data/timeseries/athlete_id=3b4e6ca7-ba55-41ce-b3a5-c212e3e1216e/part.parquet | 12.5 MB |
| data/timeseries/athlete_id=3cbfbe79-bca0-4cbb-9f47-aeeed28935ea/part.parquet | 5.0 MB |
| data/timeseries/athlete_id=3dfc1e35-974a-4fe9-930a-6f8bc3661fab/part.parquet | 7.9 MB |
| data/timeseries/athlete_id=3ebf74d4-6410-49ff-93fa-73df4547349f/part.parquet | 5.3 KB |
| data/timeseries/athlete_id=4002b745-87a9-4943-8e64-eee58750f4d5/part.parquet | 12.7 MB |
| data/timeseries/athlete_id=412f2c73-3258-4e2f-98cb-c8ad408af443/part.parquet | 9.0 MB |
| data/timeseries/athlete_id=42dd0d81-299d-4355-9272-2a01f1f90cfa/part.parquet | 21.9 MB |
| data/timeseries/athlete_id=448058a8-e556-4809-91e3-1a887cfb17a7/part.parquet | 15.4 MB |
| data/timeseries/athlete_id=46c32ff1-ffd7-4f92-9d6f-cfb2ad4c6ade/part.parquet | 3.4 MB |
| data/timeseries/athlete_id=47bf049f-6872-4be4-b394-5339ecd9c19a/part.parquet | 646.9 KB |
| data/timeseries/athlete_id=4892e1ad-2807-4213-8d03-de9fcd50fde0/part.parquet | 67.3 MB |
| data/timeseries/athlete_id=4f0c636a-4f97-42fd-9a94-313324a9206e/part.parquet | 7.0 MB |
| data/timeseries/athlete_id=4f3c4986-0d8b-45a1-95ab-9c4a63603b7b/part.parquet | 1.2 MB |
| data/timeseries/athlete_id=4fb202cb-2045-4678-8db8-293ab9d74e87/part.parquet | 691.1 KB |
| data/timeseries/athlete_id=51bf0b67-9f83-47d2-9bc1-3481b23b5162/part.parquet | 12.0 MB |
| data/timeseries/athlete_id=51deda38-0000-0000-1091-6be060020000/part.parquet | 4.6 MB |
| data/timeseries/athlete_id=5384ed2e-86ba-4e8c-bd6a-f88763411369/part.parquet | 685.6 KB |
| data/timeseries/athlete_id=567cd049-8b1f-41f1-92c6-cd53e30edf60/part.parquet | 1.9 MB |
| data/timeseries/athlete_id=592185ee-aa27-412e-99ee-a9ec84556588/part.parquet | 14.1 MB |
| data/timeseries/athlete_id=5aa3356a-8dfe-4219-a5a2-782d18373289/part.parquet | 38.2 MB |
| data/timeseries/athlete_id=5cdef3ba-3eed-4569-aa6c-689ca997263e/part.parquet | 543.3 KB |
| data/timeseries/athlete_id=5d189d4f-f66b-4711-871a-2bf055c6b8e0/part.parquet | 735.9 KB |
| data/timeseries/athlete_id=5ddaf6fa-6ed6-4b58-8783-bf9490726e00/part.parquet | 3.2 MB |
| data/timeseries/athlete_id=5e14d7e0-5117-43ce-b1d8-c08da73f60f4/part.parquet | 4.7 MB |
| data/timeseries/athlete_id=64a14779-8667-4cba-ad6a-1304fcf0bfed/part.parquet | 102.4 KB |
| data/timeseries/athlete_id=64c424af-fdbb-4359-8ea4-481a6fdcc33c/part.parquet | 339.6 KB |
| data/timeseries/athlete_id=66325c2e-4879-45fa-a755-a0dbd395bf68/part.parquet | 20.4 MB |
| data/timeseries/athlete_id=6665ea71-3488-4468-8928-d0d839cc3060/part.parquet | 55.2 KB |
| data/timeseries/athlete_id=66b727cc-3dbf-4805-86d5-5397ec8fd78d/part.parquet | 4.3 MB |
| data/timeseries/athlete_id=6a2c6e8d-5671-4011-964c-5c2b8dc80e78/part.parquet | 9.2 MB |
| data/timeseries/athlete_id=6a967b94-dd32-472c-91aa-b9e45f038626/part.parquet | 93.6 KB |
| data/timeseries/athlete_id=6da57b80-623e-4122-ae3f-a9770f0c1833/part.parquet | 32.8 MB |
| data/timeseries/athlete_id=6feaaddc-460b-4676-ad87-fbd1cf47fc9e/part.parquet | 16.3 MB |
| data/timeseries/athlete_id=71cebf49-94ca-4dff-9f2d-80fe75b8f408/part.parquet | 6.8 KB |
| data/timeseries/athlete_id=73c3336b-9633-48fb-a7fc-3ff0a190d2bf/part.parquet | 9.4 MB |
| data/timeseries/athlete_id=742feafc-ccfb-4e60-ae51-7ad4ab2e892d/part.parquet | 7.2 MB |
| data/timeseries/athlete_id=74eeaad8-342b-4c59-a948-3696af838285/part.parquet | 876.6 KB |
| data/timeseries/athlete_id=755ebfd0-501f-4da1-a33f-4fc5c71fc232/part.parquet | 981.4 KB |
| data/timeseries/athlete_id=78926b80-f0bc-4050-bf96-6198980d9616/part.parquet | 13.9 MB |
| data/timeseries/athlete_id=78debe68-6c2c-47eb-8806-6acf1dd99738/part.parquet | 363.8 KB |
| data/timeseries/athlete_id=78e230f1-b3b1-454d-9631-6e7b7460a03d/part.parquet | 736.7 KB |
| data/timeseries/athlete_id=7905b2a2-427a-482a-b3e0-f1f46c999755/part.parquet | 101.3 MB |
| data/timeseries/athlete_id=791e7b73-dc7c-486c-8f70-0dd6b5250262/part.parquet | 93.5 KB |
| data/timeseries/athlete_id=7acf08f7-6d02-40ed-97eb-55bf3b7b20a3/part.parquet | 14.9 MB |
| data/timeseries/athlete_id=7bc02014-62c1-4ccd-8da9-c17b507df5d8/part.parquet | 1.1 MB |
| data/timeseries/athlete_id=7cf5599f-bf2f-4603-abff-a206439229b5/part.parquet | 3.8 MB |
| data/timeseries/athlete_id=7da85d89-f92a-45a9-936b-e1af847c287a/part.parquet | 913.9 KB |
| data/timeseries/athlete_id=7e84e6be-1de2-4199-910c-62bfd6ee18b7/part.parquet | 7.1 MB |
| data/timeseries/athlete_id=8281a079-f0e2-47ca-b4b0-87af8ba73b6b/part.parquet | 20.9 MB |
| data/timeseries/athlete_id=82eb7d39-a33b-4bb3-a3c3-d8e47d99abd1/part.parquet | 1.5 MB |
| data/timeseries/athlete_id=8456c057-5591-4c59-9e03-a1587c09e05a/part.parquet | 109.7 MB |
| data/timeseries/athlete_id=8a9cee37-89d0-4ea4-bf36-f865c82288eb/part.parquet | 920.2 KB |
| data/timeseries/athlete_id=8bfe40af-8f5f-4987-ae42-819cd4c7a76f/part.parquet | 12.5 MB |
| data/timeseries/athlete_id=8c294198-ffa6-446c-8a42-c00f631a516c/part.parquet | 3.9 MB |
| data/timeseries/athlete_id=8e64bb82-b878-4a80-935e-1ef1e3f4127b/part.parquet | 8.3 MB |
| data/timeseries/athlete_id=8fc8f5ae-aafb-4bd2-aa3c-52bdc38a1c08/part.parquet | 6.7 MB |
| data/timeseries/athlete_id=9183284b-5aae-41ba-a6f4-605493dcb7e4/part.parquet | 2.7 MB |
| data/timeseries/athlete_id=92f8efd2-9520-456e-b1d4-b57adac1652e/part.parquet | 730.8 KB |
| data/timeseries/athlete_id=93f4f214-8868-4b91-a127-c1813a588137/part.parquet | 12.3 MB |
| data/timeseries/athlete_id=96ee8f39-8b3a-42c4-9991-23294396d373/part.parquet | 9.4 MB |
| data/timeseries/athlete_id=982a84ce-6167-4ab9-8d52-f894882cbc78/part.parquet | 6.7 MB |
| data/timeseries/athlete_id=9a417863-6ba9-485d-a30f-16c54bd9638f/part.parquet | 339.7 KB |
| data/timeseries/athlete_id=9bea6bce-9571-48fb-b5be-5a2d036b72ab/part.parquet | 8.8 MB |
| data/timeseries/athlete_id=9f2fadea-57c3-4e7f-84d1-e9949a60511f/part.parquet | 1.3 MB |
| data/timeseries/athlete_id=a0819d65-04c8-4a86-9f90-ace81e49769e/part.parquet | 14.8 MB |
| data/timeseries/athlete_id=a2cfe223-381d-4b45-8db7-cbe97ea2cbef/part.parquet | 8.1 MB |
| data/timeseries/athlete_id=a37970fe-2b33-48eb-90c3-bc5edce825db/part.parquet | 291.3 KB |
| data/timeseries/athlete_id=a40ce4b3-8c2e-4bad-8907-3a9b738f7db2/part.parquet | 1.9 MB |
| data/timeseries/athlete_id=aa54c948-457a-4c59-b9b6-ad867c3d8466/part.parquet | 144.0 KB |
| data/timeseries/athlete_id=aaa62037-5e5c-42fa-ba26-bfaf3f82169c/part.parquet | 1.6 MB |
| data/timeseries/athlete_id=aae3b3db-5c2e-4781-8dcf-5580eb11cb1f/part.parquet | 1.4 MB |
| data/timeseries/athlete_id=ab6df498-7fbb-4399-b710-9b056e5e5f37/part.parquet | 48.8 MB |
| data/timeseries/athlete_id=ae98190b-a646-43f1-ade7-f7cc3290b42b/part.parquet | 3.0 MB |
| data/timeseries/athlete_id=afced8f5-3614-4c93-9a7d-dba3da3ddcc0/part.parquet | 9.0 MB |
| data/timeseries/athlete_id=b12abc91-0625-43e4-b46d-970ca9736289/part.parquet | 12.0 MB |
| data/timeseries/athlete_id=b208ff70-2eb6-40ca-ba9f-5bb816f9c94d/part.parquet | 6.8 MB |
| data/timeseries/athlete_id=b5e4a177-ad8d-4ff0-b96b-e68b56fcdb5d/part.parquet | 29.0 MB |
| data/timeseries/athlete_id=b75175ea-0c5f-4d0c-99bd-1a8e776cb349/part.parquet | 453.3 KB |
| data/timeseries/athlete_id=b7991421-f9b8-4c9f-8572-3cfc36f11e5f/part.parquet | 986.0 KB |
| data/timeseries/athlete_id=bbf1647a-4c2c-42ac-b304-b8e9ce039c2d/part.parquet | 2.0 MB |
| data/timeseries/athlete_id=bc1b5e15-9057-4393-bab1-477d9aaa73f8/part.parquet | 33.9 MB |
| data/timeseries/athlete_id=bd3cb5b9-21db-427e-aab7-bcf485fac947/part.parquet | 7.4 MB |
| data/timeseries/athlete_id=bd59c5cc-d411-4768-8ffe-97e97bb2147d/part.parquet | 3.8 MB |
| data/timeseries/athlete_id=be9a5d39-b76a-4a6b-9d10-6f75aeac740e/part.parquet | 648.1 KB |
| data/timeseries/athlete_id=c05e83f6-0574-4c35-a549-a44bdc1fee98/part.parquet | 8.5 MB |
| data/timeseries/athlete_id=c0decac6-f2ba-454e-b25d-496d5a68b6b4/part.parquet | 51.8 MB |
| data/timeseries/athlete_id=c389bff2-b87f-4126-9f2e-7665bdf72c88/part.parquet | 11.0 MB |
| data/timeseries/athlete_id=c78b4183-022e-4efa-9300-85e7c5499c85/part.parquet | 8.5 MB |
| data/timeseries/athlete_id=c883434d-7f48-4b24-80d8-b80fc7f4724f/part.parquet | 2.0 MB |
| data/timeseries/athlete_id=ca41839c-e0cb-4f0d-8a33-eeb459493e22/part.parquet | 7.5 MB |
| data/timeseries/athlete_id=d172bbfe-2370-434c-a7d4-34fd96923f3e/part.parquet | 4.6 MB |
| data/timeseries/athlete_id=d22c1e5f-e852-49fd-964e-78015102d4b4/part.parquet | 19.7 MB |
| data/timeseries/athlete_id=d53b57b4-8278-4c8e-aadf-cbe21a0e9f9a/part.parquet | 242.7 KB |
| data/timeseries/athlete_id=d53f72ea-d2b9-44a6-b81a-7de40dfb0c27/part.parquet | 1.3 MB |
| data/timeseries/athlete_id=d5aebaaa-c1d1-4136-bceb-08fb808f907e/part.parquet | 897.7 KB |
| data/timeseries/athlete_id=d666979d-8a32-47e4-8bb4-e81f1722f2df/part.parquet | 9.2 MB |
| data/timeseries/athlete_id=d6702b3c-ffd4-49ea-a2bf-992993efca0e/part.parquet | 555.5 KB |
| data/timeseries/athlete_id=d6974aca-ea50-4e6b-95ae-9633b22b7a02/part.parquet | 582.9 KB |
| data/timeseries/athlete_id=da007600-104d-4bbc-8678-e2d396ff9ef8/part.parquet | 2.0 MB |
| data/timeseries/athlete_id=da8d5d2f-d7da-4206-9cb4-38530f52c3f1/part.parquet | 1.0 MB |
| data/timeseries/athlete_id=dc90fde7-7517-4849-9a9c-58c30f029e1a/part.parquet | 445.6 KB |
| data/timeseries/athlete_id=dd68a750-3189-44e8-9a25-3a08402dc381/part.parquet | 11.3 MB |
| data/timeseries/athlete_id=df61f1fb-4875-4b04-ad09-e60140505de4/part.parquet | 14.2 MB |
| data/timeseries/athlete_id=e0c25302-d20d-4284-a3c1-9a71b0b5c8b0/part.parquet | 12.7 MB |
| data/timeseries/athlete_id=e240364d-d569-4ddb-8576-d1f722b4c636/part.parquet | 139.4 KB |
| data/timeseries/athlete_id=e431e684-fc0b-4c7a-b07e-cdd0c3f43477/part.parquet | 3.0 MB |
| data/timeseries/athlete_id=e456feae-206c-4ec2-ad76-b05128cbdfa2/part.parquet | 17.1 MB |
| data/timeseries/athlete_id=e471380d-dbad-49ea-a77a-93849ccc1a58/part.parquet | 2.9 MB |
| data/timeseries/athlete_id=e4dbd3b3-74ef-4f2a-9743-8f28ea89f3d8/part.parquet | 8.1 MB |
| data/timeseries/athlete_id=e86d20ce-2637-400c-8fdc-ac003305fc11/part.parquet | 173.4 KB |
| data/timeseries/athlete_id=ed2047e0-eeb7-4576-ba65-4473b74090ee/part.parquet | 25.8 MB |
| data/timeseries/athlete_id=ed6ea531-edea-481f-9faf-44e47f9e8178/part.parquet | 409.5 KB |
| data/timeseries/athlete_id=ee04cfac-a3c8-4a96-9780-bf0b0999cf35/part.parquet | 2.8 MB |
| data/timeseries/athlete_id=f03ffb7a-3563-440e-9c55-af87c988e47f/part.parquet | 6.1 MB |
| data/timeseries/athlete_id=f1081033-d08f-4a0c-873e-3dca03cf33d2/part.parquet | 450.6 KB |
| data/timeseries/athlete_id=f1a7ef79-cc78-4098-91c2-145a80c31bc4/part.parquet | 350.7 KB |
| data/timeseries/athlete_id=f21e17ad-08f2-4c8b-9f2d-29c2d4d94ac2/part.parquet | 8.3 MB |
| data/timeseries/athlete_id=f3e86f4c-38cb-46e1-b8a7-ba4630f40a0a/part.parquet | 522.8 KB |
| data/timeseries/athlete_id=f43673fa-600b-4b0b-8c3a-ef627347ebe1/part.parquet | 2.3 MB |
| data/timeseries/athlete_id=f488cc96-04a8-4a60-8d56-d79d86710369/part.parquet | 1.4 MB |
| data/timeseries/athlete_id=f561df02-ad97-4507-bc03-34d1af09c0c7/part.parquet | 5.0 MB |
| data/timeseries/athlete_id=f7cf7973-ca61-451d-bffd-467652af2a30/part.parquet | 27.3 MB |
| data/timeseries/athlete_id=f993c2cf-9dd1-47b1-b3f0-26e52d9cc685/part.parquet | 4.5 MB |
| data/timeseries/athlete_id=fc4a17ff-8d46-4eeb-b220-42639b8bc7d5/part.parquet | 8.0 MB |
| data/timeseries_exceptions/exceptions.parquet | 84.7 KB |


Dataset card: `data/hf/opendata/README.md`
