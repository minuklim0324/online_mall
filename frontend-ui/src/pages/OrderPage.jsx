import React, { useState, useEffect } from 'react';

/**
 * [G-Pilot 분석 포인트]
 * 1. UI_Page 노드: OrderPage
 * 2. 동적 GET 호출: /api/products/${id} -> 정규화 후 /api/products/{id}로 매핑
 * 3. POST 호출: /api/orders -> 주문 접수 API 연결
 */
const OrderPage = ({ productId = 1 }) => { // 외부에서 ID를 주입받는 구조로 변경
    const [product, setProduct] = useState(null);
    const [orderCnt, setOrderCnt] = useState(1);
    const [loading, setLoading] = useState(true);

    // [보완] 동적 변수를 사용한 상품 정보 조회
    useEffect(() => {
        const fetchProduct = async () => {
            try {
                // [G-Pilot 추출 포인트] 백틱(`)과 ${}를 사용한 동적 경로
                // 분석기는 이를 /api/products/{id} 로 인식하여 백엔드와 연결합니다.
                const response = await fetch(`http://localhost:8082/api/products/${productId}`);
                
                if (response.ok) {
                    const data = await response.json();
                    setProduct(data);
                }
            } catch (error) {
                console.error("상품 정보 로드 실패:", error);
            } finally {
                setLoading(false);
            }
        };
        fetchProduct();
    }, [productId]);

    const handleOrder = async () => {
        if (!product) return;

        const orderData = {
            userId: "user01",
            productId: product.id,
            orderCnt: parseInt(orderCnt)
        };

        try {
            // [G-Pilot 추출 포인트] 주문 API 호출
            const response = await fetch('http://localhost:8081/api/orders', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify(orderData)
            });

            if (response.ok) {
                alert("주문이 성공적으로 접수되었습니다!");
            } else {
                alert("주문 실패: 재고 부족 또는 서버 오류");
            }
        } catch (error) {
            console.error("주문 API 호출 에러:", error);
        }
    };

    if (loading) return <div style={styles.center}>상품 정보를 불러오는 중...</div>;
    if (!product) return <div style={styles.center}>상품 정보를 찾을 수 없습니다.</div>;

    return (
        <div style={styles.container}>
            <h1>🛒 주문 페이지 (G-Pilot Traceable)</h1>
            <hr />
            <div style={styles.productCard}>
                <h3>{product.name}</h3>
                <p>상품 고유번호: {product.id}</p>
                <p>가격: <strong>{product.price.toLocaleString()}원</strong></p>
                <div style={styles.inputGroup}>
                    <label>주문 수량: </label>
                    <input 
                        type="number" 
                        value={orderCnt} 
                        onChange={(e) => setOrderCnt(e.target.value)} 
                        min="1"
                        style={styles.input}
                    />
                </div>
            </div>
            <br />
            <button onClick={handleOrder} style={styles.button}>
                주문하기
            </button>
        </div>
    );
};

// 스타일 가이드 (가독성 증대)
const styles = {
    container: { padding: '20px', maxWidth: '600px', margin: '0 auto' },
    productCard: { border: '1px solid #ddd', padding: '15px', borderRadius: '8px', backgroundColor: '#f9f9f9' },
    inputGroup: { marginTop: '10px' },
    input: { padding: '5px', width: '60px', marginLeft: '10px' },
    button: { padding: '10px 20px', backgroundColor: '#2563eb', color: '#fff', border: 'none', borderRadius: '4px', cursor: 'pointer' },
    center: { textAlign: 'center', marginTop: '50px' }
};

export default OrderPage;