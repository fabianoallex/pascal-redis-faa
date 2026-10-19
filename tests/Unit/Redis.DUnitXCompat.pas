unit Redis.DUnitXCompat;

{ Adaptador fino: expoe a API de asserts do FPCUnit (TAssert.AssertEquals,
  AssertTrue, AssertFalse, Fail) por cima do Assert do DUnitX.

  Por que isto existe. As duas suites unitarias — tests\Unit (DUnitX/Delphi) e
  tests\Unit\fpc (FPCUnit) — precisam ter a MESMA cobertura, e a forma barata de
  garantir isso e' o corpo dos testes ser identico nos dois lados, so' mudando a
  declaracao das fixtures (atributos [Test] contra secao published). Sem este
  adaptador, cada assert teria de ser reescrito na traducao, e as duas suites
  divergiriam na primeira manutencao apressada.

  Alem disso ele conserta um detalhe traicoeiro: Assert.AreEqual(string, string)
  do DUnitX ignora maiusculas POR PADRAO. Numa suite que compara codigo de erro
  ('WRONGTYPE'), formato de verbatim ('txt') e dumps em hexa, isso enfraqueceria
  os testes em silencio — e so' do lado Delphi. Aqui a comparacao de texto e'
  sempre sensivel a maiusculas, como a do FPCUnit. }

interface

uses
  SysUtils, // ExceptClass
  DUnitX.TestFramework;

type
  /// Same name and signature as FPCUnit's TRunMethod.
  TRunMethod = procedure of object;

  TAssert = class
  public
    class procedure AssertEquals(const AExpected, AActual: string); overload;
    class procedure AssertEquals(const AMessage, AExpected, AActual: string); overload;
    class procedure AssertEquals(AExpected, AActual: Integer); overload;
    class procedure AssertEquals(const AMessage: string; AExpected, AActual: Integer); overload;
    class procedure AssertEquals(AExpected, AActual: Int64); overload;
    class procedure AssertEquals(const AMessage: string; AExpected, AActual: Int64); overload;
    class procedure AssertEquals(AExpected, AActual: Currency); overload;
    class procedure AssertEquals(const AMessage: string; AExpected, AActual: Currency); overload;
    class procedure AssertEquals(AExpected, AActual, ADelta: Double); overload;
    class procedure AssertEquals(const AMessage: string; AExpected, AActual, ADelta: Double); overload;
    class procedure AssertEquals(AExpected, AActual: Boolean); overload;
    class procedure AssertEquals(const AMessage: string; AExpected, AActual: Boolean); overload;

    class procedure AssertTrue(ACondition: Boolean); overload;
    class procedure AssertTrue(const AMessage: string; ACondition: Boolean); overload;
    class procedure AssertFalse(ACondition: Boolean); overload;
    class procedure AssertFalse(const AMessage: string; ACondition: Boolean); overload;

    class procedure Fail(const AMessage: string);

    class procedure AssertException(AExceptionClass: ExceptClass; AMethod: TRunMethod); overload;
    class procedure AssertException(const AMessage: string; AExceptionClass: ExceptClass;
      AMethod: TRunMethod); overload;
  end;

implementation

class procedure TAssert.AssertEquals(const AExpected, AActual: string);
begin
  // False = case-sensitive. DUnitX's default would be True.
  Assert.AreEqual(AExpected, AActual, False);
end;

class procedure TAssert.AssertEquals(const AMessage, AExpected, AActual: string);
begin
  Assert.AreEqual(AExpected, AActual, False, AMessage);
end;

class procedure TAssert.AssertEquals(AExpected, AActual: Integer);
begin
  Assert.AreEqual(AExpected, AActual);
end;

class procedure TAssert.AssertEquals(const AMessage: string; AExpected, AActual: Integer);
begin
  Assert.AreEqual(AExpected, AActual, AMessage);
end;

class procedure TAssert.AssertEquals(AExpected, AActual: Int64);
begin
  Assert.AreEqual(AExpected, AActual);
end;

class procedure TAssert.AssertEquals(const AMessage: string; AExpected, AActual: Int64);
begin
  Assert.AreEqual(AExpected, AActual, AMessage);
end;

class procedure TAssert.AssertEquals(AExpected, AActual: Currency);
begin
  Assert.AreEqual(AExpected, AActual);
end;

class procedure TAssert.AssertEquals(const AMessage: string; AExpected, AActual: Currency);
begin
  Assert.AreEqual(AExpected, AActual, AMessage);
end;

class procedure TAssert.AssertEquals(AExpected, AActual, ADelta: Double);
begin
  Assert.AreEqual(AExpected, AActual, ADelta);
end;

class procedure TAssert.AssertEquals(const AMessage: string; AExpected, AActual, ADelta: Double);
begin
  Assert.AreEqual(AExpected, AActual, ADelta, AMessage);
end;

class procedure TAssert.AssertEquals(AExpected, AActual: Boolean);
begin
  Assert.AreEqual(AExpected, AActual);
end;

class procedure TAssert.AssertEquals(const AMessage: string; AExpected, AActual: Boolean);
begin
  Assert.AreEqual(AExpected, AActual, AMessage);
end;

class procedure TAssert.AssertTrue(ACondition: Boolean);
begin
  Assert.IsTrue(ACondition);
end;

class procedure TAssert.AssertTrue(const AMessage: string; ACondition: Boolean);
begin
  Assert.IsTrue(ACondition, AMessage);
end;

class procedure TAssert.AssertFalse(ACondition: Boolean);
begin
  Assert.IsFalse(ACondition);
end;

class procedure TAssert.AssertFalse(const AMessage: string; ACondition: Boolean);
begin
  Assert.IsFalse(ACondition, AMessage);
end;

class procedure TAssert.Fail(const AMessage: string);
begin
  Assert.Fail(AMessage);
end;

class procedure TAssert.AssertException(AExceptionClass: ExceptClass; AMethod: TRunMethod);
begin
  Assert.WillRaise(AMethod, AExceptionClass);
end;

class procedure TAssert.AssertException(const AMessage: string; AExceptionClass: ExceptClass;
  AMethod: TRunMethod);
begin
  Assert.WillRaise(AMethod, AExceptionClass, AMessage);
end;

end.
