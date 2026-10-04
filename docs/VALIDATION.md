# 本次运行验证记录

日期：2026-10-04（Asia/Shanghai）

环境：Windows 11 / Python 3.14.4 / SQLite 3.50.4。

## 已在本机实际完成

- `python -m unittest discover -v`：34 项通过；另外 30 项 WAL 参数化测试因
  SQLite 未包含 WAL-reset 修复而跳过。跳过不计为通过。
- 4 个 spawn 消费者争抢 80 个任务；完成集合与入队集合完全一致，无重复成功确认。
- 4 个 spawn 生产者使用相同 key 入队，持久化仅生成一个任务。
- 真正子进程死亡：claim 后崩溃，以及 ack 的未提交事务中崩溃，均验证恢复。
- 400 步固定种子随机 API 历史，验证状态、尝试预算、generation 和终态事件不变量。
- `experiments.faults`：两组均注入退出码 23 的进程死亡；非幂等效果=2、幂等效果=1。
- `experiments.model_check`：正确抽象通过；两个错误抽象产生反例。
- 性能矩阵 18 个 trial，共完成 2880 个任务；每组完整性与 claim 数量检查通过。
- README 的 CLI init/enqueue/work/stats 示例实际执行，sum 任务完成。

## 未验证

- 本机 WAL 模式、硬件掉电、磁盘故障、多机调度、长时间生产运行。
- GitHub Actions 配置覆盖 Linux/Windows + Python 3.11/3.14；云端执行状态需查看仓库 Actions。

原始结果在 `results/` 中。基准结果附带运行环境、重复编号和随机化种子。
测试通过是这些场景的实验证据，不是不存在任何错误的数学证明。
