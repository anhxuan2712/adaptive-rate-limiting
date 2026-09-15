# ĐỀ TÀI NGHIÊN CỨU

## Tên đề tài đề xuất

* **Tiếng Việt:** Nghiên cứu và triển khai thuật toán Sliding Window kết hợp cơ chế Adaptive Threshold cho hệ thống Rate Limiting trên Kubernetes.
* **Tiếng Anh:** Research and Implementation of a Sliding Window Algorithm Combined with Adaptive Threshold for Rate Limiting Systems on Kubernetes.

---

## 1. Lý do chọn đề tài

Trong kiến trúc microservices triển khai trên Kubernetes, hệ thống thường xuyên phải đối mặt với các biến động lớn về lưu lượng truy cập như traffic spikes hoặc các hành vi truy cập bất thường (bot, DDoS). Những yếu tố này có thể gây quá tải hệ thống, làm suy giảm hiệu năng hoặc dẫn đến gián đoạn dịch vụ nếu không có cơ chế kiểm soát phù hợp.

Các phương pháp Rate Limiting truyền thống như **Fixed Window**, **Leaky Bucket** hay **Token Bucket** đều tồn tại những hạn chế nhất định:
- **Fixed Window:** Gặp sai số tại ranh giới thời gian (boundary conditions).
- **Leaky Bucket:** Duy trì tốc độ ổn định nhưng thiếu linh hoạt với lưu lượng đột biến.
- **Token Bucket:** Phức tạp hơn trong triển khai và quản lý trạng thái phân tán.

Trong bối cảnh hệ thống phân tán và co giãn động như Kubernetes, các hạn chế này ảnh hưởng đáng kể đến hiệu quả kiểm soát lưu lượng.

Trong khi đó, thuật toán **Sliding Window** cho phép đo lường lưu lượng truy cập một cách liên tục theo thời gian, giúp cải thiện độ chính xác so với các phương pháp truyền thống. Tuy nhiên, nếu chỉ sử dụng **ngưỡng tĩnh (Static Threshold)**, hệ thống vẫn thiếu khả năng thích ứng với trạng thái tải thực tế.

Do đó, đề tài tập trung nghiên cứu và làm chủ thuật toán Sliding Window trong bài toán Rate Limiting, đồng thời kết hợp cơ chế **Adaptive Threshold** — vận hành theo mô hình vòng lặp phản hồi (*feedback control loop*):
$$\text{Quan sát lưu lượng thực tế} \longrightarrow \text{Phân tích thống kê} \longrightarrow \text{Tự động điều chỉnh ngưỡng giới hạn}$$

Giải pháp không chỉ giúp đảm bảo tính sẵn sàng (**High Availability**) mà còn góp phần nâng cao khả năng bảo vệ hệ thống (**System-level Security**) thông qua việc hạn chế các hành vi truy cập bất thường như bot, brute-force hoặc tấn công từ chối dịch vụ (DDoS), từ đó tăng khả năng chống chịu (**resilience**) trong môi trường thực tế — đồng thời không yêu cầu can thiệp vào source code của hệ thống được bảo vệ.

---

## 2. Mục tiêu nghiên cứu

### Về lý thuyết:
- Nghiên cứu và phân tích sâu thuật toán Sliding Window trong bài toán Rate Limiting, bao gồm các biến thể như **Sliding Window Log** và **Sliding Window Counter**; đánh giá ưu nhược điểm và lựa chọn phương pháp phù hợp cho môi trường hệ thống phân tán.
- Xây dựng cơ chế **Adaptive Threshold** dựa trên phương pháp thống kê **EWMA (Exponentially Weighted Moving Average)** kết hợp **độ lệch chuẩn (Standard Deviation)** của lưu lượng truy cập, nhằm điều chỉnh ngưỡng giới hạn theo biến động thực tế của traffic theo thời gian thực.

### Về thực nghiệm:
- Triển khai hệ thống Rate Limiting trên nền tảng **Kubernetes** theo hướng tách biệt hoàn toàn khỏi application, không yêu cầu thay đổi source code.
- **Sliding Window Counter** được thực thi tại tầng API Gateway để đo lường lưu lượng.
- Một thành phần điều khiển (**Adaptive Threshold Controller**) độc lập thu thập số liệu giám sát, tính toán ngưỡng mới theo công thức EWMA, và cập nhật cấu hình Gateway theo thời gian thực thông qua vòng lặp phản hồi định kỳ.

### Về đánh giá:
- Đánh giá hiệu quả của giải pháp thông qua các kịch bản kiểm thử tải khác nhau (*normal load*, *spike load*, *stress/attack simulation*), so sánh với phương pháp sử dụng ngưỡng tĩnh dựa trên các tiêu chí:
  1. **Độ trễ (Latency)**
  2. **Thông lượng (Throughput)**
  3. **Tỷ lệ request bị chặn (Reject Rate)** và **Tỷ lệ chặn nhầm request hợp lệ (False Positive Rate)**
  4. **Độ chính xác trong kiểm soát lưu lượng**
  5. **Khả năng thích ứng với biến động tải** (bao gồm tương quan với mức sử dụng CPU/độ trễ hệ thống khi tải thay đổi, đo như một chỉ số đánh giá thêm).

---

## 3. Phạm vi nghiên cứu

### 3.1. Thuật toán & Cơ chế

Đề tài tập trung vào việc làm chủ thuật toán Sliding Window với hai hướng tiếp cận chính:
* **Sliding Window Log:** Lưu trữ toàn bộ timestamp của request (độ chính xác cao nhưng tốn bộ nhớ).
* **Sliding Window Counter:** Sử dụng bộ đếm theo khoảng thời gian (tối ưu tài nguyên, phù hợp triển khai thực tế).

> **Lựa chọn trong đề tài:** Sử dụng **Sliding Window Counter**, lưu trạng thái tập trung trên **Redis** nhằm đảm bảo tính nhất quán khi API Gateway triển khai nhiều replica trong Kubernetes.

Cơ chế **Adaptive Threshold** được xây dựng dựa trên công thức:
$$\text{Threshold}(t) = \text{EWMA}(t) + k \times \text{StdDev}(t)$$

Trong đó:
* $\text{EWMA}(t)$: Phản ánh xu hướng lưu lượng gần nhất.
* $\text{StdDev}(t)$: Biểu diễn mức độ biến động của lưu lượng.
* $k$: Hệ số điều chỉnh được xác định thông qua thực nghiệm.

Cơ chế này cho phép hệ thống tự động điều chỉnh ngưỡng rate limiting nhằm cân bằng giữa việc bảo vệ hệ thống và đảm bảo trải nghiệm người dùng hợp lệ.

---

### 3.2. Kiến trúc triển khai

Hệ thống được triển khai trên nền tảng **Kubernetes** với các thành phần chính:

| Thành phần | Vai trò & Công nghệ |
| :--- | :--- |
| **Kong API Gateway** | Thực thi Rate Limiting tại tầng hạ tầng thông qua plugin `rate-limiting-advanced` (hiện thực sẵn Sliding Window Counter, cấu hình qua Admin API/YAML), tách biệt hoàn toàn khỏi application phía sau. |
| **Redis** | Lưu trạng thái đếm request của Sliding Window Counter, đảm bảo nhất quán dữ liệu khi Kong chạy nhiều Pod. |
| **Adaptive Threshold Controller** | Service viết bằng Python, triển khai dưới dạng Deployment/CronJob trong Kubernetes, chạy định kỳ (mỗi 10–30 giây): truy vấn Prometheus lấy lưu lượng $\to$ tính $\text{EWMA}(t)$ và $\text{Threshold}(t)$ $\to$ gọi Kong Admin API cập nhật cấu hình rate limit tương ứng (vòng lặp phản hồi - *feedback control loop*). |
| **Prometheus** | Thu thập và lưu trữ metrics (traffic, độ trễ, tỷ lệ lỗi) từ Kong và hệ thống. |
| **Grafana** | Trực quan hóa lưu lượng thực tế và ngưỡng threshold động theo thời gian thực. |
| **Helm Chart** | Đóng gói và quản lý triển khai toàn bộ hệ thống (Kong, Redis, Adaptive Threshold Controller, Prometheus, Grafana). |
| **Backend Service** | Ứng dụng demo (Spring Boot) đóng vai trò hệ thống được bảo vệ, không cần chỉnh sửa hay hiểu sâu source code. |

---

### 3.3. Công cụ kiểm thử

* **Apache JMeter:** Dùng để mô phỏng các kịch bản tải (*normal load*, *spike load*, *stress/attack simulation*), từ đó đánh giá hiệu quả của giải pháp Adaptive Threshold so với cơ chế ngưỡng tĩnh truyền thống.
* JMeter hỗ trợ thiết kế test plan trực quan (*Thread Group*, *Ramp-up period*, *Loop Controller*) để dựng các kịch bản traffic tăng dần hoặc đột biến, đồng thời xuất báo cáo (*HTML Report / Dashboard*) trực tiếp phục vụ phần trình bày kết quả thực nghiệm trong báo cáo.
