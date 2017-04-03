ALTER PROCEDURE PopulateOrdersDetail
@count int,
@productMaxValue int
AS
BEGIN
	DECLARE @i int
	SET @i = 0
	WHILE @i < @count
		BEGIN
			DECLARE @productId int
			SET @productId = CAST((@productMaxValue - 1) * RAND() + 1 AS int)
			INSERT INTO ordersdetail
			VALUES(@i, @productId)
			SET @i = @i + 1
		END
END;
