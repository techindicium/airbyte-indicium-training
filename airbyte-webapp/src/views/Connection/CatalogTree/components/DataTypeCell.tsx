import React from "react";
import styled from "styled-components";
import { FormattedMessage } from "react-intl";

import { Cell } from "components/SimpleTableComponents";

type DataTypeCellProps = {
  nullable?: boolean;
};

const Description = styled.div`
  color: ${({ theme }) => theme.greyColor40};
  font-size: 11px;
`;

const DataTypeCell: React.FC<DataTypeCellProps> = ({ children, nullable }) => {
  return (
    <Cell>
      {children}
      {nullable && (
        <Description>
          <FormattedMessage id="form.nullable" />
        </Description>
      )}
    </Cell>
  );
};

export default DataTypeCell;
