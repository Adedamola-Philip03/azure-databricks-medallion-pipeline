-- T-SQL has no built-in INITCAP equivalent (unlike PySpark's initcap()).
-- This function title-cases each word in a string: "mary jones" -> "Mary Jones"
USE DataEngineeringPractice;
GO

CREATE OR ALTER FUNCTION dbo.fn_InitCap (@input NVARCHAR(4000))
RETURNS NVARCHAR(4000)
AS
BEGIN
    DECLARE @result NVARCHAR(4000) = '';

    SET @input = LTRIM(RTRIM(@input));

    SELECT @result = @result +
        UPPER(LEFT(value, 1)) + LOWER(SUBSTRING(value, 2, LEN(value))) + ' '
    FROM STRING_SPLIT(@input, ' ')
    WHERE value <> '';

    RETURN LTRIM(RTRIM(@result));
END;
GO