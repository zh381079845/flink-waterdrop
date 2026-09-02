# 贡献指南

感谢关注 **Flink Waterdrop Spatial**（v0.1 preview）。

## 开发环境

- JDK 8、Maven 3.6+
- 优先改动：`plugin-flink-transform-spatial`

```bash
mvn -pl plugin-flink-transform-spatial -am test -Dskip.pmd.check=true
```

## PR 建议

1. 说明动机（性能 / 正确性 / 文档）  
2. 新 UDF 或语义变更：补单测 + 更新 `docs/sedona-sql-compatibility.md`  
3. JOIN / 物化路径：保证 **禁止裸空间笛卡尔** 护栏不被削弱  
4. 不要提交 `target/`、`.idea/`、密钥  

## 行为准则

友好、就事论事。安全问题请私下联系维护者，勿公开 0-day 细节。

## 许可

贡献默认按 [Apache License 2.0](LICENSE) 授权。
